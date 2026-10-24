"""Servidor MCP do ELK (Elasticsearch + Kibana): lê logs; a configuração do projeto vem do elk-<ambiente>.json.

Índice, campo de tempo, recorte fixo, apelidos de campos e consultas salvas ficam em
<projeto>/.kiro/config/elk/elk-<ambiente>.json; o elk.properties só tem credenciais e servidores.

Transporte: Elasticsearch direto quando `es_<ambiente>` está configurado (APIs REST oficiais); senão o Kibana
via POST /internal/search/es (API interna do Kibana, mesmo corpo Query DSL; é o primeiro lugar a revalidar num upgrade).
"""
from __future__ import annotations

import csv
import json
import re
import ssl
import threading
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Annotated

import requests
from mcp.server.mcpserver import MCPServer
from pydantic import Field
from requests.adapters import HTTPAdapter

from . import job as jb, projeto as prj
from .config import ElkConfig, default_path, load_config
from .contrato import ElkHttpError, Erro, erro, operacao

mcp = MCPServer("elk", instructions=(
    "Logs do ELK (Elasticsearch/Kibana), só leitura; a única escrita é busca salva no Kibana, após o usuário "
    "confirmar. Passe `projeto` (caminho absoluto do workspace) em toda tool: o MCP lê "
    ".kiro/config/elk/elk-<ambiente>.json e aplica sozinho índice, campo de tempo, `fixos` (recorte do sistema) e "
    "apelidos de campo (nivel, mensagem, servico...: valem em campos, consulta, retornar e agrupar_por); consulta "
    "salva vai em `salva` pelo título. Sem esse arquivo, pergunte uma vez: configurar (elk_configurar) ou seguir "
    "passando `indice` real de elk_listar_indices. Nunca invente índice, campo ou valor: venha de uma tool.\n"
    "Ambiente: prod (padrão), homol ou dev; diga ambiente e janela na resposta. `inicio` é obrigatório ('now-1h' ou "
    "ISO com fuso; horário falado é de Brasília, -03:00): comece curto, o volume é de milhões por hora.\n"
    "Volume: conte antes (elk_contar). Na conversa só poucos documentos (elk_buscar_logs: limite padrão 10, até 100, "
    "`retornar` só com os campos necessários). Resultado grande ou análise de muitos documentos: elk_exportar_csv "
    "grava o CSV em disco; leia só as primeiras linhas (head -n 5) para ver as colunas e filtre, conte ou agrupe com "
    "um script Python (módulo csv) que imprime só o resultado. Nunca leia o CSV inteiro nem pagine com várias buscas. "
    "O script vai num arquivo .py ao lado do CSV, rodado com `python3 <arquivo>.py` e apagado no fim: nunca "
    "`python3 - <<EOF` (heredoc no stdin trava no fish).\n"
    "Incidente/causa raiz: elk_contar com comparar='1d' (mesma janela no dia anterior) por nivel; depois só ERROR, por "
    "servico, logger e excecao: o que é novo ou cresceu é o suspeito; para achar o início, fatie a janela; evidência: "
    "elk_buscar_logs limite 5, mais_recentes=false; separe fato (contagem) de hipótese.\n"
    "Job Spring Batch (\"logs do job X\", \"último job executado\"): elk_job (sumário por step + WARN/ERROR), do "
    "ELK ou, se o pedido disser local, do arquivo de log em log_local.\n"
    "Configurar: elk_listar_indices → o usuário escolhe; recorte (fixos) pelo nome do sistema (amostra com "
    "elk_buscar_logs limite 1, confirme com elk_contar); apelidos a partir de 1–3 documentos; valores com "
    "elk_contar(agrupar_por); mostre o JSON, confirme e grave com elk_configurar.\n"
    "Logs têm dados pessoais: mostre só o necessário. Nunca leia ~/.config/mcp-elk/elk.properties (tem a senha).\n"
    "Resposta: {\"ok\":true,\"dados\":...} ou {\"ok\":false,\"erro\":{\"codigo\",\"detalhe\",\"campo\"}}; siga o "
    "detalhe do erro, sem tentativas às cegas. elk_job devolve Markdown."))

TIMEOUT = 120
LIMITE_TETO = 100
PAGINA = 1000
MAX_RESULT_WINDOW = 10_000  # index.max_result_window padrão do ES
TEXTO_MAX = 1500
LINHAS_MAX = 15  # stack trace: só o começo
MAX_JOB = 50_000  # eventos de listener lidos por execução
MAX_JOB_ERROS = 5_000
FOLGA = "5s"
ALIASES = {
    "prod": "prod", "producao": "prod", "p": "prod",
    "homol": "homol", "homologacao": "homol", "hml": "homol", "h": "homol",
    "dev": "dev", "desenvolvimento": "dev", "desenv": "dev", "d": "dev",
}

Projeto = Annotated[str | None, Field(description="caminho absoluto do workspace")]
Ambiente = Annotated[str | None, Field(description="prod (padrão), homol ou dev")]
Indice = Annotated[str | None, Field(description="nome em indices da config (padrão: indice-padrao) ou padrão real")]
Inicio = Annotated[str, Field(description="'now-1h' ou ISO 8601 com fuso")]
Consulta = Annotated[str | None, Field(description="Lucene; KQL simples é convertido")]
Campos = Annotated[dict | None, Field(description="{campo ou apelido: valor} exato; '*' no valor = wildcard")]
Filtros = Annotated[list[dict] | None, Field(description="Query DSL")]
Salva = Annotated[str | None, Field(description="título (config) ou id de consulta salva do Kibana")]


def _enxuto(schema):
    """Tira do inputSchema o que só repete o nome (title) e o `anyOf [X, null]`/`default null` dos opcionais: o
    schema vai em toda sessão."""
    if isinstance(schema, list):
        return [_enxuto(s) for s in schema]
    if not isinstance(schema, dict):
        return schema
    s = {k: _enxuto(v) for k, v in schema.items() if k != "title" and not (k == "default" and v is None)}
    if (alt := s.get("anyOf")) and {"type": "null"} in alt and len(alt) == 2:
        s.pop("anyOf")
        s.update(next(a for a in alt if a != {"type": "null"}))
    return s


def tool(fn):
    """Registra a tool com o envelope do contrato (um TextContent, sem outputSchema) e devolve `fn` intacta."""
    mcp.tool(structured_output=False)(operacao(fn))
    registrada = mcp._tool_manager._tools[fn.__name__]
    registrada.parameters = _enxuto(registrada.parameters)
    return fn


# ---------------------------------------------------------------- HTTP

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
            raise Erro("certificado",
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
            try:
                _instance = ElkSession(load_config())
            except (OSError, ValueError) as exc:
                raise Erro("credencial", str(exc)) from exc
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
        raise Erro("entrada", f"Ambiente desconhecido: '{ambiente}'. Use prod/produção (padrão), homol/homologação ou "
                              "dev/desenvolvimento.", "ambiente")
    return ALIASES[sem_acento]


def _kibana(amb: str) -> str:
    url = get_session().cfg.kibana.get(amb)
    if not url:
        raise Erro("credencial", f"Ambiente '{amb}' sem URL no elk.properties: adicione '{amb} = https://...'.",
                   "ambiente")
    return url


def _base(amb: str, espaco: str) -> str:
    return _kibana(amb) if espaco == "default" else f"{_kibana(amb)}/s/{espaco}"


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
    return get_session().request("GET", f"{_base(amb, espaco)}/api/saved_objects/_find", params=params)["saved_objects"]


# ---------------------------------------------------------------- config do projeto

_APELIDO = re.compile(r'("(?:[^"\\]|\\.)*")|(?<![\w.@-])([\w]+)(?=\s*:)')


@dataclass
class _Alvo:
    amb: str
    indice: str
    tempo: str
    fixos: dict = field(default_factory=dict)
    apelidos: dict = field(default_factory=dict)
    cfg: dict | None = None
    avisos: list = field(default_factory=list)

    def campo(self, nome: str) -> str:
        return self.apelidos.get(nome, nome)

    def campos(self, campos: dict | None) -> dict:
        return {self.campo(k): v for k, v in (campos or {}).items()}

    def consulta(self, consulta: str | None) -> str | None:
        """Apelido antes de ':' vira o campo real (fora de aspas)."""
        if not consulta or not self.apelidos:
            return consulta
        return _APELIDO.sub(lambda m: m[1] or self.campo(m[2]), consulta)

    def exige(self, apelido: str) -> str:
        if apelido not in self.apelidos:
            raise Erro("config", f"apelido '{apelido}' ausente em indices.<nome>.campos da config (elk_configurar)",
                       f"campos.{apelido}")
        return self.apelidos[apelido]


def _alvo(projeto: str | None, ambiente: str | None, indice: str | None, campo_tempo: str | None = None) -> _Alvo:
    amb = _ambiente(ambiente)
    cfg = prj.carregar(projeto, amb) if projeto else None
    if cfg:
        nome = indice or cfg["indice-padrao"]
        idx = cfg["indices"].get(nome) or next((i for i in cfg["indices"].values() if i["padrao"] == nome), None)
        if idx:
            return _Alvo(amb, idx["padrao"], campo_tempo or idx["campo-tempo"], dict(idx.get("fixos", {})),
                         dict(idx.get("campos", {})), cfg)
        return _Alvo(amb, nome, campo_tempo or "@timestamp", cfg=cfg,
                     avisos=[f"índice '{nome}' fora da config: sem fixos nem apelidos"])
    if not indice:
        onde = "no projeto" if projeto else "(informe projeto)"
        raise Erro("config", f"sem .kiro/config/elk/elk-{amb}.json {onde}: informe `indice` (padrao de "
                             "elk_listar_indices) ou configure com elk_configurar", "indice")
    return _Alvo(amb, indice, campo_tempo or "@timestamp")


# ---------------------------------------------------------------- consulta

_OPERADOR = re.compile(r'("(?:[^"\\]|\\.)*")|\b(and|or|not)\b|([\w.@-]+)\s*(>=|<=|>|<)\s*', re.IGNORECASE)
LINGUAGENS = {"kuery": "kuery", "kql": "kuery", "lucene": "lucene"}
_PARAMETROS = re.compile(r"Par[âa]metros:\s*([^\n]+)", re.I)


def _lucene(consulta: str) -> str:
    """KQL simples -> Lucene, fora de aspas: operadores em maiúsculas (no Lucene `and` vira termo e o resultado
    explode) e comparação `campo > 0` -> `campo:>0`. Lucene puro passa intacto."""
    def troca(m):
        if m.group(1):
            return m.group(1)
        return m.group(2).upper() if m.group(2) else f"{m.group(3)}:{m.group(4)}"
    return _OPERADOR.sub(troca, consulta)


def _query(inicio: str, fim: str, consulta: str | None, campos: dict | None, filtros: list | None,
           campo_tempo: str) -> dict:
    if not (consulta or campos or filtros):
        raise Erro("entrada", "Consulta vazia: informe `consulta`, `campos`, `filtros` ou `salva`.", "consulta")
    must = [{"query_string": {"query": _lucene(consulta)}}] if consulta else []
    filtro = [{"wildcard": {campo: {"value": valor}}} if isinstance(valor, str) and "*" in valor
              else {"term": {campo: valor}} for campo, valor in (campos or {}).items()]
    filtro += list(filtros or [])
    filtro.append({"range": {campo_tempo: {"gte": inicio, "lte": fim}}})
    return {"bool": {"must": must, "filter": filtro}}


def _preparar(projeto, ambiente, indice, inicio, fim, consulta, campos, filtros, salva, campo_tempo
              ) -> tuple[_Alvo, dict]:
    """Alvo + Query DSL: fixos + campos (apelidos resolvidos) + consulta salva (com os parâmetros dela)."""
    parametros: list[str] = []
    if salva:
        s = _consulta_salva(projeto, _ambiente(ambiente), salva)
        indice = indice or s["indice"]
        consulta = " AND ".join(f"({_lucene(c)})" for c in (s["consulta"], consulta) if c) or None
        filtros = s["filtros"] + list(filtros or [])
        if m := _PARAMETROS.search(s["descricao"] or ""):
            parametros = [p.strip() for p in m[1].split(",") if p.strip()]
    alvo = _alvo(projeto, ambiente, indice, campo_tempo)
    reais = alvo.campos(campos)
    if parametros:
        if faltam := [p for p in parametros if p not in reais]:
            raise Erro("entrada", f"a consulta salva '{salva}' exige parâmetro(s): {', '.join(faltam)} (em `campos`)",
                       "campos")
        # o filtro salvo do parâmetro tem um valor de exemplo: somado ao pedido, zera o resultado
        filtros = [f for f in filtros if not any(f'"{p}"' in json.dumps(f) for p in parametros)]
    return alvo, _query(inicio, fim, alvo.consulta(consulta), {**alvo.fixos, **reais}, filtros, alvo.tempo)


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
    """Só o começo de textos longos: LINHAS_MAX linhas (stack trace) e TEXTO_MAX caracteres."""
    if not isinstance(v, str):
        return v
    linhas = v.split("\n")
    if len(linhas) > LINHAS_MAX:
        v = "\n".join(linhas[:LINHAS_MAX]) + f"\n… (+{len(linhas) - LINHAS_MAX} linhas)"
    return v[:TEXTO_MAX] + "…" if len(v) > TEXTO_MAX else v


def _resumo(alvo: _Alvo, inicio, fim) -> dict:
    out = {"ambiente": alvo.amb, "indice": alvo.indice, "janela": {"inicio": inicio, "fim": fim}}
    return out | ({"avisos": alvo.avisos} if alvo.avisos else {})


def _documentos(alvo: _Alvo, query: dict, retornar: list | None, max_linhas: int, estado: dict):
    """Documentos achatados, do mais antigo ao mais novo, paginando até `max_linhas`. `estado` recebe total, linhas
    e truncado. ES direto: PIT + search_after; só Kibana: search_after por tempo, descartando _ids repetidos."""
    s, es, pit = get_session(), _es(alvo.amb), None
    body = {"size": PAGINA, "track_total_hits": True, "query": query}
    estado.update(linhas=0, truncado=False)
    if retornar:
        body["_source"] = retornar
    if es:  # PIT + _shard_doc: paginação oficial, sem perda nem duplicata
        pit = s.request("POST", f"{es}/{alvo.indice}/_pit", params={"keep_alive": "2m"})["id"]
        body["sort"] = [{alvo.tempo: "asc"}, {"_shard_doc": "asc"}]
    else:
        body["sort"] = [{alvo.tempo: "asc"}]
    borda, vistos = None, set()  # sem PIT: _ids já lidos no último timestamp
    try:
        while True:
            if pit:
                body["pit"] = {"id": pit, "keep_alive": "2m"}
            resp = _search(alvo.amb, None if pit else alvo.indice, body)
            pit = resp.get("pit_id", pit)
            if "total" not in estado:
                estado["total"] = _total(resp)
                body["track_total_hits"] = False
            hits = resp.get("hits", {}).get("hits", [])
            for h in hits:
                if h["_id"] in vistos:
                    continue
                if estado["linhas"] >= max_linhas:
                    estado["truncado"] = True
                    return
                estado["linhas"] += 1
                yield _flat(h.get("_source", {}))
            if len(hits) < body["size"]:
                return
            ultimo = hits[-1]["sort"]
            if pit:
                body["search_after"] = ultimo
                continue
            # sem PIT: recomeça em ts >= último ts e descarta os _ids já lidos nesse ts;
            # a página cresce com os vistos para sempre trazer documentos novos
            if ultimo[0] != borda:
                borda, vistos = ultimo[0], set()
            vistos |= {h["_id"] for h in hits if h["sort"][0] == borda}
            body["search_after"] = [borda - 1]
            body["size"] = PAGINA + len(vistos)
            if body["size"] > MAX_RESULT_WINDOW:
                raise Erro("entrada", f"Documentos demais com o mesmo {alvo.tempo} ({borda}): reduza a janela ou "
                                      "configure es_<ambiente> no elk.properties (ES direto, com PIT).")
    finally:
        if pit and es:
            try:
                s.request("DELETE", f"{es}/_pit", json={"id": pit})
            except Exception:  # noqa: BLE001 - o PIT expira sozinho em 2m
                pass


# ---------------------------------------------------------------- tools: logs

@tool
def elk_buscar_logs(inicio: Inicio, projeto: Projeto = None, consulta: Consulta = None, campos: Campos = None,
                    filtros: Filtros = None, salva: Salva = None, fim: str = "now", limite: int = 10,
                    mais_recentes: bool = True,
                    retornar: Annotated[list[str] | None, Field(
                        description="campos de cada item; padrão: tempo + nivel, servico, logger, mensagem da config; "
                                    "['*'] = todos")] = None,
                    indice: Indice = None, campo_tempo: str | None = None, ambiente: Ambiente = None) -> dict:
    """Poucos documentos para ler na conversa (limite ≤ 100), textos cortados no começo. Para muitos: elk_exportar_csv.
    → dados: {ambiente, indice, janela, total, retornados, itens[{_id, campo: valor}]}"""
    if not 1 <= limite <= LIMITE_TETO:
        raise Erro("entrada", f"`limite` deve estar entre 1 e {LIMITE_TETO}; para mais, use elk_exportar_csv "
                              "e filtre o CSV com um script Python.", "limite")
    alvo, query = _preparar(projeto, ambiente, indice, inicio, fim, consulta, campos, filtros, salva, campo_tempo)
    if retornar is None and alvo.apelidos:
        retornar = [alvo.tempo] + [alvo.apelidos[a] for a in ("nivel", "servico", "logger", "mensagem")
                                   if a in alvo.apelidos]
    body = {"size": limite, "track_total_hits": True, "query": query,
            "sort": [{alvo.tempo: {"order": "desc" if mais_recentes else "asc"}}]}
    if retornar:
        body["_source"] = [alvo.campo(c) for c in retornar]
    resp = _search(alvo.amb, alvo.indice, body)
    itens = [{"_id": h["_id"], **{k: _cut(v) for k, v in _flat(h.get("_source", {})).items()}}
             for h in resp.get("hits", {}).get("hits", [])]
    total = _total(resp)
    out = _resumo(alvo, inicio, fim) | {"total": total, "retornados": len(itens), "itens": itens}
    if (total or 0) > LIMITE_TETO:
        out["proximo"] = "para analisar todos: elk_exportar_csv e um script Python sobre o CSV"
    return out


def _desloca(t: str, delta: str) -> str:
    """Data math do ES: 'now-1h' -> 'now-1h-1d'; ISO -> 'ISO||-1d'."""
    return f"{t}-{delta}" if t.startswith("now") else f"{t}||-{delta}"


@tool
def elk_contar(inicio: Inicio, projeto: Projeto = None, consulta: Consulta = None, campos: Campos = None,
               filtros: Filtros = None, salva: Salva = None, fim: str = "now",
               agrupar_por: Annotated[str | None, Field(description="campo ou apelido agregável (keyword)")] = None,
               top: int = 10,
               comparar: Annotated[str | None, Field(
                   description="deslocamento da referência, ex.: '1d', '7d', '2h' (mesma janela antes)")] = None,
               indice: Indice = None, campo_tempo: str | None = None, ambiente: Ambiente = None) -> dict:
    """Conta sem trazer documentos; `agrupar_por` dá os `top` valores mais frequentes; `comparar` conta também a
    janela de referência. → dados: {ambiente, indice, janela, total, grupos[{valor, total, ref?, novo?}],
    referencia?{inicio, fim, total}}"""
    alvo, query = _preparar(projeto, ambiente, indice, inicio, fim, consulta, campos, filtros, salva, campo_tempo)
    campo = alvo.campo(agrupar_por) if agrupar_por else None

    def contar(q: dict, size: int) -> tuple[int | None, dict]:
        body = {"size": 0, "track_total_hits": True, "query": q}
        if campo:
            body["aggs"] = {"grupos": {"terms": {"field": campo, "size": size}}}
        resp = _search(alvo.amb, alvo.indice, body)
        buckets = resp.get("aggregations", {}).get("grupos", {}).get("buckets", [])
        return _total(resp), {b["key"]: b["doc_count"] for b in buckets}

    total, grupos = contar(query, top)
    out = _resumo(alvo, inicio, fim) | {"total": total}
    if campo:
        out["grupos"] = [{"valor": k, "total": n} for k, n in grupos.items()]
    if comparar:
        ri, rf = _desloca(inicio, comparar), _desloca(fim, comparar)
        rq = json.loads(json.dumps(query))
        rq["bool"]["filter"][-1] = {"range": {alvo.tempo: {"gte": ri, "lte": rf}}}
        rtotal, rgrupos = contar(rq, top * 5)
        out["referencia"] = {"inicio": ri, "fim": rf, "total": rtotal}
        for g in out.get("grupos", []):
            g["ref"] = rgrupos.get(g["valor"], 0)
            g["novo"] = g["ref"] == 0
    return out


def _cell(v):
    return json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v


def _relativo(path: Path, projeto: str | None) -> str:
    try:
        return path.relative_to(projeto).as_posix() if projeto else str(path)
    except ValueError:
        return str(path)


@tool
def elk_exportar_csv(inicio: Inicio, projeto: Projeto = None,
                     assunto: Annotated[str | None, Field(
                         description="kebab-case; nome do arquivo em download.path da config")] = None,
                     arquivo: Annotated[str | None, Field(description="caminho do CSV (sem config)")] = None,
                     consulta: Consulta = None, campos: Campos = None, filtros: Filtros = None, salva: Salva = None,
                     fim: str = "now", retornar: list[str] | None = None, max_linhas: int = 100_000,
                     indice: Indice = None, campo_tempo: str | None = None, ambiente: Ambiente = None) -> dict:
    """Grava em CSV (UTF-8 com BOM, do mais antigo ao mais novo) todos os documentos, sem passá-los pela conversa.
    Colunas = `retornar` ou os campos da 1ª página. Arquivo existente é sobrescrito. → dados: {arquivo, linhas,
    total, truncado, colunas}"""
    alvo, query = _preparar(projeto, ambiente, indice, inicio, fim, consulta, campos, filtros, salva, campo_tempo)
    if arquivo:
        path = Path(arquivo)
        if not path.is_absolute():
            if not projeto:
                raise Erro("entrada", f"`arquivo` deve ser um caminho absoluto: {arquivo}", "arquivo")
            path = prj.raiz(projeto) / path
    else:
        pasta = ((alvo.cfg or {}).get("download") or {}).get("path")
        if not (pasta and assunto):
            raise Erro("entrada", "informe `assunto` (com download.path na config) ou `arquivo`", "arquivo")
        path = prj.raiz(projeto) / pasta / f"{datetime.now():%Y%m%d-%H%M}_{assunto}.csv"
    colunas = [alvo.campo(c) for c in retornar or []]
    estado: dict = {}
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, colunas, extrasaction="ignore") if colunas else None
            if writer:
                writer.writeheader()
            for row in _documentos(alvo, query, colunas or None, max_linhas, estado):
                if writer is None:
                    colunas = list(row)
                    writer = csv.DictWriter(f, colunas, extrasaction="ignore")
                    writer.writeheader()
                writer.writerow({k: _cell(v) for k, v in row.items()})
    except Exception as exc:
        if (e := erro(exc)) is None:
            raise
        raise Erro(e.codigo, f"Exportação interrompida após {estado.get('linhas', 0)} linha(s) gravadas em "
                             f"{path} (arquivo parcial): {e.detalhe}", e.campo) from exc
    out = {"arquivo": _relativo(path, projeto), "linhas": estado["linhas"], "total": estado.get("total"),
           "truncado": estado["truncado"], "colunas": colunas}
    if estado["linhas"]:
        out["proximo"] = ("não leia o CSV inteiro: head -n 5 para ver as colunas; filtre, conte ou agrupe com um "
                          "script Python (módulo csv) que imprime só o resultado")
    return out | ({"avisos": alvo.avisos} if alvo.avisos else {})


@tool
def elk_job(projeto: Projeto = None,
            job: Annotated[str | None, Field(description="nome do job; sem ele, o último executado")] = None,
            inicio: str = "now-7d", fim: str = "now",
            log_local: Annotated[str | None, Field(
                description="caminho absoluto do log local (JSONL ou texto); sem ele, ELK")] = None,
            erros: Annotated[bool, Field(description="varre WARN/ERROR da janela da execução")] = True,
            indice: Indice = None, ambiente: Ambiente = None) -> str:
    """Última execução de um job Spring Batch (listeners "...LoggerListener"): sumário por step em ordem, partições
    consolidadas, e WARN/ERROR agrupados. No ELK usa os apelidos mensagem, logger, thread e nivel da config.
    → Markdown pronto para mostrar"""
    if log_local:
        eventos, nome = jb.recortar(jb.ler_local(log_local), job)
        errs = [e for e in eventos if e["level"].upper() in ("WARN", "WARNING", "ERROR")] if erros else None
        return jb.relatorio([e for e in eventos if "LoggerListener" in e["logger"]], errs, "local")
    alvo = _alvo(projeto, ambiente, indice)
    msg, logger = alvo.exige("mensagem"), alvo.exige("logger")
    thread, nivel = alvo.apelidos.get("thread"), alvo.apelidos.get("nivel")
    # frase exata não casa por causa da análise de texto do índice: wildcard por tokens
    alvo_job = re.sub(r"([^\w-])", r"\\\1", job) if job else ""
    marco = f"{logger}:*LoggerListener* AND {msg}:*processamento*do*JOB*{alvo_job}*"
    resp = _search(alvo.amb, alvo.indice, {"size": 50, "sort": [{alvo.tempo: {"order": "desc"}}],
                                           "_source": [alvo.tempo, msg],
                                           "query": _query(inicio, fim, marco, alvo.fixos, None, alvo.tempo)})
    marcos = [(r.get(alvo.tempo), r.get(msg) or "") for h in resp.get("hits", {}).get("hits", [])
              for r in [_flat(h.get("_source", {}))]]
    ini = next(((ts, m[1]) for ts, texto in marcos if (m := jb.INICIO_JOB.search(texto)) and m[1] == (job or m[1])),
               None)
    if not ini:
        return f"Nenhuma execução {'do job ' + job if job else 'de job'} entre {inicio} e {fim} ({alvo.amb})."
    fins = [ts for ts, texto in marcos if ts >= ini[0] and (m := jb.FIM_JOB.search(texto)) and m[1] == ini[1]]
    j_ini, j_fim = _desloca(ini[0], FOLGA), f"{min(fins)}||+{FOLGA}" if fins else fim
    ret = [c for c in (alvo.tempo, thread, logger, msg) if c]
    estado: dict = {}
    q = _query(j_ini, j_fim, f"{logger}:*LoggerListener*", alvo.fixos, None, alvo.tempo)
    eventos = [{"ts": r.get(alvo.tempo, ""), "thread": r.get(thread, "") if thread else "", "logger": r.get(logger, ""),
                "msg": r.get(msg) or "", "level": ""} for r in _documentos(alvo, q, ret, MAX_JOB, estado)]
    eventos, _ = jb.recortar(eventos, ini[1])
    avisos = [f"mais de {MAX_JOB} eventos de listener: sumário parcial"] if estado["truncado"] else []
    if not thread:
        avisos.append("sem apelido 'thread' na config: partições intercaladas podem não se separar")
    errs = None
    if erros and nivel:
        excecao = alvo.apelidos.get("excecao")
        q = _query(j_ini, j_fim, f"{nivel}:(WARN OR WARNING OR ERROR)", alvo.fixos, None, alvo.tempo)
        ret = [c for c in (alvo.tempo, nivel, logger, msg, excecao) if c]
        estado = {}
        errs = [{"ts": r.get(alvo.tempo, ""), "level": str(r.get(nivel, "")), "logger": r.get(logger, ""),
                 "msg": r.get(msg) or "", "excecao": r.get(excecao) if excecao else None}
                for r in _documentos(alvo, q, ret, MAX_JOB_ERROS, estado)]
        if estado["truncado"]:
            avisos.append(f"WARN/ERROR: só os primeiros {MAX_JOB_ERROS} de {estado.get('total')}")
    elif erros:
        avisos.append("sem apelido 'nivel' na config: WARN/ERROR não varridos")
    return jb.relatorio(eventos, errs, alvo.amb, " · ".join(avisos) or None)


# ---------------------------------------------------------------- tools: consultas salvas

def _filtros_kibana(filters: list[dict]) -> list[dict]:
    """Filtros do Kibana ({meta, query, $state}) -> Query DSL; ignora os desativados e aplica `negate`."""
    out = []
    for f in filters or []:
        meta = f.get("meta", {})
        if meta.get("disabled"):
            continue
        dsl = f["query"] if "query" in f else {k: v for k, v in f.items() if k not in ("meta", "$state")}
        if not dsl:  # filtro vazio do Kibana ({"query": {}}): o ES recusa com 400
            continue
        out.append({"bool": {"must_not": [dsl]}} if meta.get("negate") else dsl)
    return out


@tool
def elk_listar_consultas(projeto: Projeto = None, busca: str | None = None, espaco: str | None = None,
                         limite: int = 50, ambiente: Ambiente = None) -> dict:
    """Buscas salvas do Discover (`search`) e saved queries (`query`) do Kibana; `busca` = início de palavra do título;
    sem `espaco`, todos. → dados: {total, itens[{id, espaco, tipo, titulo}], config[títulos já na config]}"""
    amb = _ambiente(ambiente)
    objetos = []
    for sp in _espacos(amb, espaco):
        objetos += [o | {"espaco": sp} for o in _saved_objects(amb, sp, ["search", "query"], busca)]
    out = {"total": len(objetos), "itens": [{"id": o["id"], "espaco": o["espaco"], "tipo": o["type"],
                                             "titulo": o["attributes"].get("title")} for o in objetos[:limite]]}
    if projeto and (cfg := prj.carregar(projeto, amb)):
        out["config"] = sorted(cfg.get("consultas", {}))
    return out


def _obter_saved_object(amb: str, id: str, espaco: str | None = None) -> tuple[dict, str]:
    """Saved object por ID: só em `espaco`, se informado; senão em todos os spaces."""
    for sp in _espacos(amb, espaco):
        for tipo in ("search", "query"):
            try:
                return get_session().request("GET", f"{_base(amb, sp)}/api/saved_objects/{tipo}/{id}"), sp
            except ElkHttpError as e:
                if e.status_code != 404:
                    raise
    raise LookupError(f"Consulta não encontrada: {id}")


def _detalhar(amb: str, id: str, espaco: str | None) -> dict:
    obj, sp = _obter_saved_object(amb, id, espaco)
    attrs = obj["attributes"]
    indice = None
    if obj["type"] == "search":
        src = json.loads(attrs.get("kibanaSavedObjectMeta", {}).get("searchSourceJSON") or "{}")
        query, filters = src.get("query", {}), src.get("filter", [])
        ref = next((r["id"] for r in obj.get("references", []) if r.get("type") == "index-pattern"), None)
        if ref:
            views = {o["id"]: o["attributes"] for o in _saved_objects(amb, sp, ["index-pattern"], None)}
            indice = views.get(ref, {}).get("title")
    else:
        query, filters = attrs.get("query", {}), attrs.get("filters", [])
    return {"id": obj["id"], "espaco": sp, "tipo": obj["type"], "titulo": attrs.get("title"),
            "descricao": attrs.get("description") or None, "consulta": query.get("query") or None,
            "linguagem": query.get("language"), "filtros": _filtros_kibana(filters), "indice": indice,
            "colunas": attrs.get("columns")}


def _consulta_salva(projeto: str | None, amb: str, salva: str) -> dict:
    """Título das `consultas` da config (sem diferenciar caixa) ou id do Kibana."""
    consultas = ((prj.carregar(projeto, amb) if projeto else None) or {}).get("consultas", {})
    ref = consultas.get(salva) or next((v for k, v in consultas.items() if k.lower() == salva.lower()), None)
    return _detalhar(amb, ref["id"], ref.get("espaco")) if ref else _detalhar(amb, salva, None)


@tool
def elk_obter_consulta(salva: Annotated[str, Field(description="título (config) ou id")], projeto: Projeto = None,
                       espaco: str | None = None, ambiente: Ambiente = None) -> dict:
    """Detalhes de uma consulta salva (para ver ou alterar; para rodar, passe `salva` nas outras tools).
    → dados: {id, espaco, tipo, titulo, descricao, consulta, linguagem, filtros, indice, colunas}"""
    amb = _ambiente(ambiente)
    return _detalhar(amb, salva, espaco) if espaco else _consulta_salva(projeto, amb, salva)


def _filtro_discover(i: int, campo: str | None, valor, dsl: dict | None = None) -> dict:
    """Filtro no formato que o Discover grava (aparece como pílula): `phrase` para {campo: valor}; `custom` para
    wildcard e Query DSL crua."""
    meta = {"alias": None, "disabled": False, "negate": False,
            "indexRefName": f"kibanaSavedObjectMeta.searchSourceJSON.filter[{i}].meta.index"}
    if dsl is None and not (isinstance(valor, str) and "*" in valor):
        return {"meta": meta | {"key": campo, "field": campo, "params": {"query": valor}, "type": "phrase"},
                "query": {"match_phrase": {campo: valor}}, "$state": {"store": "appState"}}
    dsl = dsl or {"wildcard": {campo: {"value": valor}}}
    return {"meta": meta | {"type": "custom", "key": "query", "value": json.dumps(dsl, ensure_ascii=False)},
            "query": dsl, "$state": {"store": "appState"}}


_REF_INDICE = "kibanaSavedObjectMeta.searchSourceJSON.index"


def _data_view_id(amb: str, espaco: str, indice: str) -> str:
    view = next((o for o in _saved_objects(amb, espaco, ["index-pattern"], None)
                 if o["attributes"].get("title") == indice), None)
    if not view:
        raise LookupError(f"Data view '{indice}' não existe no espaço '{espaco}': use o `padrao` e o `espaco` de "
                          "elk_listar_indices.")
    return view["id"]


def _linguagem(linguagem: str) -> str:
    lang = LINGUAGENS.get(linguagem.strip().lower())
    if not lang:
        raise Erro("entrada", f"Linguagem desconhecida: '{linguagem}'. Use kuery (KQL) ou lucene.", "linguagem")
    return lang


def _filtros_discover(campos: dict | None, filtros: list | None) -> list[dict]:
    itens = [(c, v, None) for c, v in (campos or {}).items()] + [(None, None, f) for f in (filtros or [])]
    return [_filtro_discover(i, *item) for i, item in enumerate(itens)]


def _corpo_search(view_id: str, titulo: str, consulta: str | None, campos: dict | None, filtros: list | None,
                  colunas: list | None, descricao: str | None, lang: str) -> dict:
    filtro = _filtros_discover(campos, filtros)
    source = {"query": {"query": consulta or "", "language": lang}, "filter": filtro, "indexRefName": _REF_INDICE}
    refs = [_REF_INDICE] + [f["meta"]["indexRefName"] for f in filtro]
    return {
        "attributes": {"title": titulo, "description": descricao or "", "columns": colunas or [],
                       "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps(source, ensure_ascii=False)}},
        "references": [{"name": r, "type": "index-pattern", "id": view_id} for r in refs],
    }


def _registrar(projeto: str | None, alvo: _Alvo, titulo: str, id: str, espaco: str, antigo: str | None = None):
    """Grava `consultas[titulo]` na config do projeto (se houver), tirando o título antigo do mesmo id."""
    if not (projeto and alvo.cfg):
        return
    cfg = json.loads(json.dumps(alvo.cfg))
    consultas = {k: v for k, v in cfg.get("consultas", {}).items() if not (k == antigo and v["id"] == id)}
    consultas[titulo] = {"id": id} | ({"espaco": espaco} if espaco != "default" else {})
    prj.gravar(projeto, alvo.amb, cfg | {"consultas": consultas})


@tool
def elk_criar_consulta(titulo: str, projeto: Projeto = None, consulta: str | None = None, campos: Campos = None,
                       filtros: Filtros = None, colunas: list[str] | None = None,
                       descricao: Annotated[str | None, Field(
                           description="'Parâmetros: <campo>, ...' marca os filtros que mudam a cada execução")] = None,
                       espaco: str = "default", linguagem: str = "kuery", indice: Indice = None,
                       ambiente: Ambiente = None) -> dict:
    """Cria busca salva do Discover ligada ao data view (só após o usuário confirmar). Os fixos da config e `campos`
    viram filtros (pílulas); `consulta` vai na barra (kuery ou lucene). Com config, registra o título nela.
    → dados: {id, titulo, espaco}"""
    if not (consulta or campos or filtros):
        raise Erro("entrada", "Informe `consulta`, `campos` ou `filtros`.", "consulta")
    lang = _linguagem(linguagem)
    alvo = _alvo(projeto, ambiente, indice)
    body = _corpo_search(_data_view_id(alvo.amb, espaco, alvo.indice), titulo, alvo.consulta(consulta),
                         {**alvo.fixos, **alvo.campos(campos)}, filtros, [alvo.campo(c) for c in colunas or []],
                         descricao, lang)
    resp = get_session().request("POST", f"{_base(alvo.amb, espaco)}/api/saved_objects/search", json=body)
    _registrar(projeto, alvo, titulo, resp["id"], espaco)
    return {"id": resp["id"], "titulo": titulo, "espaco": espaco}


@tool
def elk_atualizar_consulta(id: str, projeto: Projeto = None, espaco: str = "default", titulo: str | None = None,
                           consulta: str | None = None, campos: Campos = None, filtros: Filtros = None,
                           colunas: list[str] | None = None, descricao: str | None = None,
                           linguagem: str | None = None, indice: Indice = None, ambiente: Ambiente = None) -> dict:
    """Atualiza busca salva (`search`) só no que for informado (após confirmar antes → depois); preserva ordenação,
    período e layout. `campos`/`filtros` substituem todos os filtros (fixos da config somados); consulta="" limpa a
    barra. → dados: {id, titulo, espaco}"""
    if all(v is None for v in (titulo, indice, consulta, campos, filtros, colunas, descricao, linguagem)):
        raise Erro("entrada", "Nada a atualizar: informe ao menos um campo.")
    alvo = _alvo(projeto, ambiente, indice) if (projeto or indice) else _Alvo(_ambiente(ambiente), "", "")
    url = f"{_base(alvo.amb, espaco)}/api/saved_objects/search/{id}"
    try:
        obj = get_session().request("GET", url)
    except ElkHttpError as e:
        if e.status_code == 404:
            raise LookupError(f"Busca salva '{id}' não encontrada no espaço '{espaco}' (só o tipo `search` é "
                              "atualizável; use elk_listar_consultas).") from e
        raise
    attrs = obj["attributes"]
    antigo = attrs.get("title")
    src = json.loads(attrs.get("kibanaSavedObjectMeta", {}).get("searchSourceJSON") or "{}")
    refs = obj.get("references", [])
    view_id = _data_view_id(alvo.amb, espaco, alvo.indice) if indice else next(
        (r["id"] for r in refs if r["name"] == _REF_INDICE), None)
    if consulta is not None or linguagem:
        atual = src.get("query", {})
        src["query"] = {"query": atual.get("query", "") if consulta is None else alvo.consulta(consulta),
                        "language": _linguagem(linguagem) if linguagem else atual.get("language", "kuery")}
    if campos is not None or filtros is not None:
        src["filter"] = _filtros_discover({**alvo.fixos, **alvo.campos(campos)}, filtros)
        refs = [{"name": n, "type": "index-pattern", "id": view_id}
                for n in [_REF_INDICE] + [f["meta"]["indexRefName"] for f in src["filter"]]]
    elif indice:
        refs = [r | {"id": view_id} if r.get("type") == "index-pattern" else r for r in refs]
    src["indexRefName"] = _REF_INDICE
    novos = {"title": titulo, "description": descricao,
             "columns": [alvo.campo(c) for c in colunas] if colunas is not None else None}
    attrs = attrs | {k: v for k, v in novos.items() if v is not None}
    attrs["kibanaSavedObjectMeta"] = {**attrs.get("kibanaSavedObjectMeta", {}),
                                      "searchSourceJSON": json.dumps(src, ensure_ascii=False)}
    get_session().request("PUT", url, json={"attributes": attrs, "references": refs})
    if titulo and titulo != antigo:
        _registrar(projeto, alvo, titulo, id, espaco, antigo)
    return {"id": id, "titulo": attrs.get("title"), "espaco": espaco}


# ---------------------------------------------------------------- tools: descoberta e configuração

@tool
def elk_listar_indices(busca: str | None = None, espaco: str | None = None, limite: int = 50,
                       ambiente: Ambiente = None) -> dict:
    """Data views do Kibana; `busca` = início de palavra do título; sem `espaco`, todos.
    → dados: {total, itens[{espaco, nome, padrao (vai em indice), campo_tempo}]}"""
    amb = _ambiente(ambiente)
    out = {}
    for sp in _espacos(amb, espaco):
        for o in _saved_objects(amb, sp, ["index-pattern"], busca):
            a = o["attributes"]
            out[(sp, a["title"])] = {"espaco": sp, "nome": a.get("name") or a["title"], "padrao": a["title"],
                                     "campo_tempo": a.get("timeFieldName")}
    itens = sorted(out.values(), key=lambda d: (d["espaco"], d["padrao"]))
    return {"total": len(itens), "itens": itens[:limite]}


@tool
def elk_listar_campos(busca: Annotated[str | None, Field(description="trecho do nome")] = None,
                      projeto: Projeto = None, limite: int = 100, indice: Indice = None,
                      ambiente: Ambiente = None) -> dict:
    """Campos do índice; `agregavel` serve para agrupar_por e filtro exato. Índices têm milhares: use `busca`.
    → dados: {total, itens[{campo, tipo, agregavel}]}"""
    alvo = _alvo(projeto, ambiente, indice)
    s = get_session()
    if es := _es(alvo.amb):
        caps = s.request("GET", f"{es}/{alvo.indice}/_field_caps", params={"fields": "*"})["fields"]
        campos = [{"campo": nome, "tipo": "|".join(tipos), "agregavel": any(t.get("aggregatable") for t in tipos.values())}
                  for nome, tipos in caps.items()]
    else:
        # API interna do Kibana 8.x (a pública /api/index_patterns/... responde 404)
        resp = s.request("GET", f"{_kibana(alvo.amb)}/internal/data_views/_fields_for_wildcard",
                         params={"pattern": alvo.indice}, headers={"elastic-api-version": "1"})
        campos = [{"campo": f["name"], "tipo": "|".join(f.get("esTypes") or [f.get("type")]),
                   "agregavel": bool(f.get("aggregatable"))} for f in resp["fields"]]
    itens = sorted((c for c in campos if not c["campo"].startswith("_") and (not busca or busca in c["campo"])),
                   key=lambda c: c["campo"])
    return {"total": len(itens), "itens": itens[:limite]}


@tool
def elk_status(projeto: Projeto = None, ambiente: Ambiente = None) -> dict:
    """Confere credencial, servidor e a config do projeto no ambiente. → dados: {usuario, ambientes[], es_direto[],
    conexao, config?{arquivo, existe, faltando[], indice_padrao, indices{nome: padrao}, apelidos[], consultas[],
    download}}"""
    amb = _ambiente(ambiente)
    cred = get_session().cfg
    out = {"usuario": cred.user, "ambientes": sorted(cred.kibana), "es_direto": sorted(cred.es)}
    try:
        out["conexao"] = f"ok ({len(_espacos(amb, None))} spaces em {amb})"
    except Exception as exc:
        if (e := erro(exc)) is None:
            raise
        out["conexao"] = f"{e.codigo}: {e.detalhe}"
    if projeto:
        arq = prj.arquivo(projeto, amb)
        cfg = {"arquivo": _relativo(arq, projeto), "existe": arq.is_file()}
        if cfg["existe"]:
            try:
                dados = json.loads(arq.read_text("utf-8"))
            except ValueError as exc:
                dados = None
                cfg["faltando"] = [f"JSON inválido: {exc}"]
            if isinstance(dados, dict):
                idx = dados.get("indices") or {}
                cfg |= {"faltando": prj.faltando(dados), "indice_padrao": dados.get("indice-padrao"),
                        "indices": {n: i.get("padrao") for n, i in idx.items()},
                        "apelidos": sorted((idx.get(dados.get("indice-padrao")) or {}).get("campos", {})),
                        "consultas": sorted(dados.get("consultas") or {}),
                        "download": (dados.get("download") or {}).get("path")}
        out["config"] = cfg
    return out


@tool
def elk_configurar(projeto: Annotated[str, Field(description="caminho absoluto do workspace")],
                   config: Annotated[dict, Field(description=(
                       "{indice-padrao, indices{nome: {padrao, campo-tempo, fixos?{campo: valor}, "
                       "campos?{apelido: campo}, valores?{apelido: [..]}}}, consultas?{titulo: {id, espaco?}}, "
                       "download?{path relativo}}"))],
                   ambiente: Ambiente = None) -> dict:
    """Valida e grava a config do ambiente (.kiro/config/elk/elk-<ambiente>.json), substituindo o arquivo: envie o
    JSON completo, já mostrado e confirmado pelo usuário; todo campo e valor veio de uma tool. → dados: {arquivo}"""
    amb = _ambiente(ambiente)
    return {"arquivo": _relativo(prj.gravar(projeto, amb, config), projeto)}


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
