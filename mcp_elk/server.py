"""Servidor MCP de leitura para ELK (Elasticsearch + Kibana), agnóstico de índice e campos.

Índice, campos e consultas chegam como parâmetro; o elk.properties só tem credenciais e servidores.

Transporte: Elasticsearch direto quando `es_<ambiente>` está configurado (APIs REST oficiais); senão o Kibana
via POST /internal/search/es (API interna do Kibana, mesmo corpo Query DSL; é o primeiro lugar a revalidar num upgrade).
"""
from __future__ import annotations

import csv
import functools
import json
import re
import threading
import unicodedata
from pathlib import Path

import requests
import ssl
from requests.adapters import HTTPAdapter
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError

from .config import ElkConfig, default_path, load_config

mcp = MCPServer(
    "elk",
    instructions=(
        "Leitura de logs no ELK (Elasticsearch/Kibana). Toda consulta exige `indice` e `inicio` (janela de tempo: "
        "'now-15m' ou ISO 8601). `ambiente` aceita prod/produção, homol/homologação ou dev/desenvolvimento; sem ele, "
        "produção. Descubra índices com elk_listar_indices, campos com elk_listar_campos e consultas prontas do Kibana "
        "com elk_listar_consultas. Dimensione com elk_contar antes de buscar; volume grande vai para "
        "elk_exportar_csv (grava em disco, nada passa pela conversa)."
    ),
)

EXPECTED = (RuntimeError, OSError, LookupError, ValueError)
TIMEOUT = 120
LIMITE_TETO = 100
PAGINA = 1000
MAX_RESULT_WINDOW = 10_000  # index.max_result_window padrão do ES
TEXTO_MAX = 2000
ALIASES = {
    "prod": "prod", "producao": "prod", "p": "prod",
    "homol": "homol", "homologacao": "homol", "hml": "homol", "h": "homol",
    "dev": "dev", "desenvolvimento": "dev", "desenv": "dev", "d": "dev",
}


def tool(fn):
    """Registra `fn` como tool (ToolError com o texto da exceção) e devolve `fn` intacta."""
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except EXPECTED as exc:
            raise ToolError(str(exc)) from exc

    mcp.tool()(wrapper)
    return fn


# ---------------------------------------------------------------- HTTP

class ElkHttpError(RuntimeError):
    def __init__(self, resp: requests.Response):
        super().__init__(f"HTTP {resp.status_code} em {resp.request.method} {resp.url}: {resp.text[:500].strip()}")
        self.status_code = resp.status_code


def ca_bundle() -> Path:
    """CAs extras (ex.: cadeia interna do Kibana), ao lado do elk.properties."""
    return default_path().parent / "elk-bundle.pem"


class _TlsAdapter(HTTPAdapter):
    """CAs do sistema (no Windows, o repositório do Windows) + elk-bundle.pem, se existir.
    Cifras padrão do OpenSSL: a lista do Python/urllib3 exclui AES256-SHA256 (RSA estático), única oferecida por
    alguns Kibana internos (handshake failure)."""

    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        if ca_bundle().is_file():
            ctx.load_verify_locations(ca_bundle())
        ctx.set_ciphers("DEFAULT")
        super().init_poolmanager(*args, ssl_context=ctx, **kwargs)


class ElkSession:
    def __init__(self, cfg: ElkConfig):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.mount("https://", _TlsAdapter())
        self.session.auth = (cfg.user, cfg.password)
        self.session.headers.update({"kbn-xsrf": "true", "Accept": "application/json"})

    def request(self, method: str, url: str, **kwargs) -> dict:
        try:
            resp = self.session.request(method, url, timeout=TIMEOUT, **kwargs)
        except requests.exceptions.SSLError as exc:
            raise RuntimeError(
                f"Certificado do servidor não confiável em {url}. Instale a CA na máquina ou grave a cadeia de "
                f"certificados (PEM) em {ca_bundle()} (ou aponte REQUESTS_CA_BUNDLE para ela) e reinicie o MCP. "
                f"Detalhe: {exc}") from exc
        if not resp.ok:
            raise ElkHttpError(resp)
        try:
            return resp.json() if resp.content else {}
        except ValueError as exc:
            raise ValueError(f"Resposta não é JSON em {method} {url}: {resp.text[:300]}") from exc


_instance: ElkSession | None = None
_lock = threading.Lock()


def get_session() -> ElkSession:
    """Sessão única por processo; carrega o elk.properties na primeira chamada."""
    global _instance
    with _lock:
        if _instance is None:
            _instance = ElkSession(load_config())
        return _instance


def set_session(session: ElkSession | None) -> None:
    """Substitui a sessão global (testes)."""
    global _instance
    _instance = session


def _ambiente(ambiente: str | None) -> str:
    if not ambiente:
        return "prod"
    sem_acento = unicodedata.normalize("NFKD", ambiente.strip().lower()).encode("ascii", "ignore").decode()
    if sem_acento not in ALIASES:
        raise ValueError(f"Ambiente desconhecido: '{ambiente}'. Use prod/produção (padrão), homol/homologação ou "
                         "dev/desenvolvimento.")
    return ALIASES[sem_acento]


def _kibana(amb: str) -> str:
    url = get_session().cfg.kibana.get(amb)
    if not url:
        raise ValueError(f"Ambiente '{amb}' sem URL no elk.properties: adicione '{amb} = https://...'.")
    return url


def _es(amb: str) -> str | None:
    return get_session().cfg.es.get(amb)


def _search(amb: str, indice: str | None, body: dict) -> dict:
    """POST _search pelo ES direto ou pelo Kibana. `indice=None` só com PIT (ES direto)."""
    s = get_session()
    if es := _es(amb):
        return s.request("POST", f"{es}/{indice}/_search" if indice else f"{es}/_search", json=body)
    resp = s.request("POST", f"{_kibana(amb)}/internal/search/es", json={"params": {"index": indice, "body": body}})
    return resp.get("rawResponse", resp)


def _espacos(amb: str, espaco: str | None) -> list[str]:
    """`espaco` informado, ou todos os spaces do Kibana visíveis ao usuário."""
    if espaco:
        return [espaco]
    return [sp["id"] for sp in get_session().request("GET", f"{_kibana(amb)}/api/spaces/space")]


def _saved_objects(amb: str, espaco: str, tipos: list[str], busca: str | None) -> list[dict]:
    params = {"type": tipos, "per_page": 10000}
    if busca:
        params |= {"search": f"{busca}*", "search_fields": "title"}
    base = _kibana(amb) if espaco == "default" else f"{_kibana(amb)}/s/{espaco}"
    return get_session().request("GET", f"{base}/api/saved_objects/_find", params=params)["saved_objects"]


# ---------------------------------------------------------------- consulta

_OPERADOR = re.compile(r'("(?:[^"\\]|\\.)*")|\b(and|or|not)\b', re.IGNORECASE)


def _lucene(consulta: str) -> str:
    """Operadores em maiúsculas fora de aspas: no KQL `and` é operador, no Lucene vira termo (e o resultado explode)."""
    return _OPERADOR.sub(lambda m: m.group(1) or m.group(2).upper(), consulta)


def _query(inicio: str, fim: str, consulta: str | None, campos: dict | None, filtros: list | None,
           campo_tempo: str) -> dict:
    if not (consulta or campos or filtros):
        raise ValueError("Consulta vazia: informe `consulta`, `campos` ou `filtros`.")
    must = [{"query_string": {"query": _lucene(consulta)}}] if consulta else []
    filtro = [{"term": {campo: valor}} for campo, valor in (campos or {}).items()]
    filtro += list(filtros or [])
    filtro.append({"range": {campo_tempo: {"gte": inicio, "lte": fim}}})
    return {"bool": {"must": must, "filter": filtro}}


def _total(resp: dict) -> int | None:
    total = resp.get("hits", {}).get("total")
    return total.get("value") if isinstance(total, dict) else total


def _flat(src: dict, prefix: str = "") -> dict:
    """{'a': {'b': 1}} -> {'a.b': 1}; listas ficam como estão."""
    out = {}
    for k, v in src.items():
        if isinstance(v, dict):
            out |= _flat(v, f"{prefix}{k}.")
        else:
            out[f"{prefix}{k}"] = v
    return out


def _cut(v):
    return v[:TEXTO_MAX] + "…" if isinstance(v, str) and len(v) > TEXTO_MAX else v


def _janela(amb, indice, inicio, fim) -> dict:
    return {"ambiente": amb, "indice": indice, "janela": {"inicio": inicio, "fim": fim}}


# ---------------------------------------------------------------- tools

@tool
def elk_buscar_logs(indice: str, inicio: str, consulta: str | None = None, campos: dict[str, str] | None = None,
                    filtros: list[dict] | None = None, fim: str = "now", limite: int = 20, mais_recentes: bool = True,
                    retornar: list[str] | None = None, campo_tempo: str = "@timestamp",
                    ambiente: str | None = None) -> dict:
    """Busca documentos (só leitura). `consulta` é Lucene (query_string): `campo:valor AND msg:*timeout*`
    (and/or/not viram maiúsculas fora de aspas, então KQL simples funciona; `campo:{...}` aninhado não).
    `campos` = filtros exatos {campo: valor} (term); `filtros` = Query DSL
    crua (como vem de elk_listar_consultas). `inicio`/`fim`: 'now-15m', 'now-1d' ou ISO 8601.
    `limite` ≤ 100; `retornar` limita os campos de cada item. Itens achatados ({'a.b': v}), textos cortados em 2000.
    Para muitos documentos use elk_exportar_csv."""
    if not 1 <= limite <= LIMITE_TETO:
        raise ValueError(f"`limite` deve estar entre 1 e {LIMITE_TETO}; para mais, use elk_exportar_csv.")
    amb = _ambiente(ambiente)
    body = {
        "size": limite,
        "track_total_hits": True,
        "sort": [{campo_tempo: {"order": "desc" if mais_recentes else "asc"}}],
        "query": _query(inicio, fim, consulta, campos, filtros, campo_tempo),
    }
    if retornar:
        body["_source"] = retornar
    resp = _search(amb, indice, body)
    itens = [{"_id": h["_id"], "_index": h["_index"], **{k: _cut(v) for k, v in _flat(h.get("_source", {})).items()}}
             for h in resp.get("hits", {}).get("hits", [])]
    return {**_janela(amb, indice, inicio, fim), "total": _total(resp), "retornados": len(itens), "itens": itens}


@tool
def elk_contar(indice: str, inicio: str, consulta: str | None = None, campos: dict[str, str] | None = None,
               filtros: list[dict] | None = None, fim: str = "now", agrupar_por: str | None = None, top: int = 10,
               campo_tempo: str = "@timestamp", ambiente: str | None = None) -> dict:
    """Conta documentos sem trazê-los (só leitura). Mesmos filtros de elk_buscar_logs. `agrupar_por` (campo
    agregável, ex.: keyword) devolve os `top` valores mais frequentes: use para dimensionar um problema ou para
    conhecer os valores reais de um campo."""
    amb = _ambiente(ambiente)
    body = {"size": 0, "track_total_hits": True, "query": _query(inicio, fim, consulta, campos, filtros, campo_tempo)}
    if agrupar_por:
        body["aggs"] = {"grupos": {"terms": {"field": agrupar_por, "size": top}}}
    resp = _search(amb, indice, body)
    out = {**_janela(amb, indice, inicio, fim), "total": _total(resp)}
    if agrupar_por:
        out["grupos"] = [{"valor": b["key"], "total": b["doc_count"]}
                         for b in resp.get("aggregations", {}).get("grupos", {}).get("buckets", [])]
    return out


def _filtros_kibana(filters: list[dict]) -> list[dict]:
    """Filtros do Kibana ({meta, query, $state}) -> Query DSL; ignora os desativados e aplica `negate`."""
    out = []
    for f in filters or []:
        meta = f.get("meta", {})
        if meta.get("disabled"):
            continue
        dsl = f.get("query") or {k: v for k, v in f.items() if k not in ("meta", "$state")}
        out.append({"bool": {"must_not": [dsl]}} if meta.get("negate") else dsl)
    return out


@tool
def elk_listar_consultas(busca: str | None = None, espaco: str | None = None, limite: int = 50,
                         ambiente: str | None = None) -> dict:
    """Consultas prontas do Kibana: buscas salvas do Discover (tipo `search`) e saved queries (`query`).
    `busca` filtra pelo início das palavras do título; `espaco` = space do Kibana (sem ele, todos).
    Devolve {total, itens[:limite]}; total > limite → refine a `busca`. Cada item traz `consulta`, `filtros`
    (Query DSL) e `indice` (padrão do data view, só em `search`), prontos para elk_buscar_logs/elk_contar/
    elk_exportar_csv. Se `linguagem` = kuery, a consulta é KQL e vai como Lucene (operadores normalizados; `campo:{...}` não funciona)."""
    amb = _ambiente(ambiente)
    objetos, views = [], {}
    for sp in _espacos(amb, espaco):
        achados = [o | {"espaco": sp} for o in _saved_objects(amb, sp, ["search", "query"], busca)]
        if any(o["type"] == "search" for o in achados[:limite]):
            views |= {o["id"]: o["attributes"] for o in _saved_objects(amb, sp, ["index-pattern"], None)}
        objetos += achados
    out = []
    for o in objetos[:limite]:
        attrs, indice = o["attributes"], None
        if o["type"] == "search":
            src = json.loads(attrs.get("kibanaSavedObjectMeta", {}).get("searchSourceJSON") or "{}")
            query, filters = src.get("query", {}), src.get("filter", [])
            ref = next((r["id"] for r in o.get("references", []) if r.get("type") == "index-pattern"), None)
            indice = views.get(ref, {}).get("title")
        else:
            query, filters = attrs.get("query", {}), attrs.get("filters", [])
        out.append({
            "id": o["id"], "espaco": o["espaco"], "tipo": o["type"], "titulo": attrs.get("title"), "descricao": attrs.get("description") or None,
            "consulta": query.get("query") or None, "linguagem": query.get("language"),
            "filtros": _filtros_kibana(filters), "indice": indice, "colunas": attrs.get("columns"),
        })
    return {"total": len(objetos), "itens": out}


@tool
def elk_listar_indices(busca: str | None = None, espaco: str | None = None, ambiente: str | None = None) -> list[dict]:
    """Data views do Kibana: [{espaco, nome, padrao (use como `indice`), campo_tempo}], sem repetir padrão no mesmo
    space. `busca` filtra pelo início das palavras do título; `espaco` = space do Kibana (sem ele, todos)."""
    amb = _ambiente(ambiente)
    out = {}
    for sp in _espacos(amb, espaco):
        for o in _saved_objects(amb, sp, ["index-pattern"], busca):
            a = o["attributes"]
            out[(sp, a["title"])] = {"espaco": sp, "nome": a.get("name") or a["title"], "padrao": a["title"],
                                     "campo_tempo": a.get("timeFieldName")}
    return sorted(out.values(), key=lambda d: (d["espaco"], d["padrao"]))


@tool
def elk_listar_campos(indice: str, busca: str | None = None, ambiente: str | None = None) -> list[dict]:
    """Campos do índice: [{campo, tipo, agregavel}]. `busca` filtra por trecho do nome. `agregavel` = serve para
    `agrupar_por` e `campos` (filtro exato)."""
    amb = _ambiente(ambiente)
    s = get_session()
    if es := _es(amb):
        caps = s.request("GET", f"{es}/{indice}/_field_caps", params={"fields": "*"})["fields"]
        campos = [{"campo": nome, "tipo": "|".join(tipos), "agregavel": any(t.get("aggregatable") for t in tipos.values())}
                  for nome, tipos in caps.items()]
    else:
        # API interna do Kibana 8.x (a pública /api/index_patterns/... responde 404)
        resp = s.request("GET", f"{_kibana(amb)}/internal/data_views/_fields_for_wildcard",
                         params={"pattern": indice}, headers={"elastic-api-version": "1"})
        campos = [{"campo": f["name"], "tipo": "|".join(f.get("esTypes") or [f.get("type")]),
                   "agregavel": bool(f.get("aggregatable"))} for f in resp["fields"]]
    return sorted((c for c in campos if not c["campo"].startswith("_") and (not busca or busca in c["campo"])),
                  key=lambda c: c["campo"])


def _cell(v):
    return json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v


@tool
def elk_exportar_csv(indice: str, inicio: str, arquivo: str, consulta: str | None = None,
                     campos: dict[str, str] | None = None, filtros: list[dict] | None = None, fim: str = "now",
                     retornar: list[str] | None = None, max_linhas: int = 100_000, campo_tempo: str = "@timestamp",
                     ambiente: str | None = None) -> dict:
    """Exporta para CSV todos os documentos da consulta, do mais antigo ao mais novo, sem passar pela conversa.
    Mesmos filtros de elk_buscar_logs. `arquivo` = caminho absoluto (pastas criadas, arquivo sobrescrito; UTF-8
    com BOM). Colunas = `retornar` ou os campos achatados da primeira página. Para em `max_linhas` (`truncado`).
    Devolve só o resumo: {arquivo, linhas, total, truncado, colunas}."""
    path = Path(arquivo)
    if not path.is_absolute():
        raise ValueError(f"`arquivo` deve ser um caminho absoluto: {arquivo}")
    amb = _ambiente(ambiente)
    s = get_session()
    body = {"size": PAGINA, "track_total_hits": True,
            "query": _query(inicio, fim, consulta, campos, filtros, campo_tempo)}
    if retornar:
        body["_source"] = retornar
    es = _es(amb)
    pit = None
    if es:  # PIT + _shard_doc: paginação oficial, sem perda nem duplicata
        pit = s.request("POST", f"{es}/{indice}/_pit", params={"keep_alive": "2m"})["id"]
        body["sort"] = [{campo_tempo: "asc"}, {"_shard_doc": "asc"}]
    else:
        body["sort"] = [{campo_tempo: "asc"}]

    path.parent.mkdir(parents=True, exist_ok=True)
    linhas, total, colunas, truncado = 0, None, list(retornar or []), False
    borda, vistos = None, set()  # sem PIT: _ids já gravados no último timestamp
    try:
        with path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, colunas, extrasaction="ignore") if colunas else None
            if writer:
                writer.writeheader()
            while not truncado:
                if pit:
                    body["pit"] = {"id": pit, "keep_alive": "2m"}
                resp = _search(amb, None if pit else indice, body)
                pit = resp.get("pit_id", pit)
                if total is None:
                    total = _total(resp)
                    body["track_total_hits"] = False
                hits = resp.get("hits", {}).get("hits", [])
                novos = [h for h in hits if h["_id"] not in vistos]
                for h in novos:
                    row = _flat(h.get("_source", {}))
                    if writer is None:
                        colunas = list(row)
                        writer = csv.DictWriter(f, colunas, extrasaction="ignore")
                        writer.writeheader()
                    writer.writerow({k: _cell(v) for k, v in row.items()})
                    linhas += 1
                    if linhas >= max_linhas:
                        truncado = linhas < (total or 0)
                        break
                if len(hits) < body["size"] or linhas >= max_linhas:
                    break
                ultimo = hits[-1]["sort"]
                if pit:
                    body["search_after"] = ultimo
                    continue
                # sem PIT: recomeça em ts >= último ts e descarta os _ids já gravados nesse ts;
                # a página cresce com os vistos para sempre trazer documentos novos
                if ultimo[0] != borda:
                    borda, vistos = ultimo[0], set()
                vistos |= {h["_id"] for h in hits if h["sort"][0] == borda}
                body["search_after"] = [borda - 1]
                body["size"] = PAGINA + len(vistos)
                if body["size"] > MAX_RESULT_WINDOW:
                    raise RuntimeError(f"Documentos demais com o mesmo {campo_tempo} ({borda}): configure "
                                       "es_<ambiente> (ES direto, com PIT) para exportar esta consulta.")
    except EXPECTED as exc:
        raise RuntimeError(f"Exportação interrompida após {linhas} linha(s) gravadas em {path}: {exc}") from exc
    finally:
        if pit and es:
            try:
                s.request("DELETE", f"{es}/_pit", json={"id": pit})
            except EXPECTED:
                pass  # o PIT expira sozinho em 2m
    return {"arquivo": str(path), "linhas": linhas, "total": total, "truncado": truncado, "colunas": colunas}


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
