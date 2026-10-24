import csv
import json
from pathlib import Path

import pytest
import requests
from requests.adapters import BaseAdapter

from mcp_elk import config, server
from mcp_elk import job as jb
from mcp_elk.config import ElkConfig
from mcp_elk.contrato import Erro

KIBANA = "https://kibana.test"
ES = "https://es.test"
HIT = {"_id": "1", "_index": "logs-1", "_source": {"@timestamp": "2026-10-08T10:00:00Z", "app": {"nome": "x"},
                                                   "message": "m" * 3000}}


class FakeAdapter(BaseAdapter):
    """routes: {(METODO, url_sem_query): handler(request) -> (status, dict|bytes) ou tupla}."""

    def __init__(self, routes=None):
        super().__init__()
        self.routes = routes or {}
        self.calls: list[requests.PreparedRequest] = []

    def send(self, request, **kwargs):
        self.calls.append(request)
        route = self.routes.get((request.method, request.url.split("?")[0]))
        status, body = (route(request) if callable(route) else route) if route else (404, b"not found")
        resp = requests.Response()
        resp.status_code = status
        resp._content = body if isinstance(body, bytes) else json.dumps(body).encode()
        resp.url, resp.request = request.url, request
        return resp

    def close(self):
        pass


def body(req) -> dict:
    return json.loads(req.body)


@pytest.fixture
def fake():
    adapter = FakeAdapter()
    session = server.ElkSession(ElkConfig(user="joao", password="segredo", kibana={"prod": KIBANA, "homol": KIBANA}))
    session.session.mount("https://", adapter)
    server.set_session(session)
    yield adapter
    server.set_session(None)


@pytest.fixture
def fake_es(fake):
    server.get_session().cfg.es["prod"] = ES
    return fake


# ---------------------------------------------------------------- config

def write(tmp_path: Path, text: str) -> Path:
    p = tmp_path / "elk.properties"
    p.write_text(text, encoding="utf-8")
    return p


def test_load_config(tmp_path):
    cfg = config.load_config(write(tmp_path, "[DEFAULT]\nuser = u\npassword = p%s#s\nprod = https://k/\n"
                                             "es_prod = https://e:9200\n"))
    assert (cfg.user, cfg.password, cfg.kibana, cfg.es) == ("u", "p%s#s", {"prod": "https://k"},
                                                            {"prod": "https://e:9200"})
    assert "p%s#s" not in repr(cfg)


def test_load_config_errors(tmp_path):
    with pytest.raises(ValueError, match="prod"):
        config.load_config(write(tmp_path, "[DEFAULT]\nuser = u\npassword = p\n"))
    with pytest.raises(FileNotFoundError):
        config.load_config(tmp_path / "nao-existe")


# ---------------------------------------------------------------- ambiente e consulta

@pytest.mark.parametrize("entrada,esperado", [(None, "prod"), ("Produção", "prod"), ("homologação", "homol"),
                                              ("HML", "homol"), ("desenvolvimento", "dev")])
def test_ambiente(entrada, esperado):
    assert server._ambiente(entrada) == esperado


def test_ambiente_desconhecido_e_sem_url(fake):
    with pytest.raises(Erro, match="desconhecido"):
        server._ambiente("qa")
    with pytest.raises(Erro, match="'dev' sem URL"):
        server.elk_contar("now-1h", indice="logs-*", consulta="x", ambiente="dev")


def test_lucene():
    assert server._lucene('a: x and b: "y and z" or not c') == 'a: x AND b: "y and z" OR NOT c'
    assert server._lucene("android:1 AND brand:x") == "android:1 AND brand:x"
    assert server._lucene('n > 0 and dt.x >= 10 and m: "a > b"') == 'n:>0 AND dt.x:>=10 AND m: "a > b"'
    assert server._lucene("n:>0 AND n:[1 TO 5]") == "n:>0 AND n:[1 TO 5]"


def test_query():
    q = server._query("now-1h", "now", "msg:erro", {"sis.nome": "a", "sis.cat": "b"}, [{"exists": {"field": "f"}}],
                      "ts")
    assert q == {"bool": {"must": [{"query_string": {"query": "msg:erro"}}], "filter": [
        {"term": {"sis.nome": "a"}}, {"term": {"sis.cat": "b"}}, {"exists": {"field": "f"}},
        {"range": {"ts": {"gte": "now-1h", "lte": "now"}}}]}}
    with pytest.raises(Erro, match="vazia"):
        server._query("now-1h", "now", None, None, None, "ts")
    q = server._query("now-1h", "now", None, {"sistema.nome": "app*", "n": 1}, None, "ts")
    assert q["bool"]["filter"][:2] == [{"wildcard": {"sistema.nome": {"value": "app*"}}}, {"term": {"n": 1}}]


# ---------------------------------------------------------------- busca e contagem

def test_buscar_logs_kibana(fake):
    fake.routes[("POST", f"{KIBANA}/internal/search/es")] = (200, {"rawResponse": {
        "hits": {"total": {"value": 7}, "hits": [HIT]}}})
    out = server.elk_buscar_logs("now-15m", indice="logs-*", campos={"sis": "a"}, limite=5, retornar=["message"],
                                 ambiente="homologação")
    sent = body(fake.calls[0])
    assert sent["params"]["index"] == "logs-*"
    assert sent["params"]["body"]["size"] == 5 and sent["params"]["body"]["_source"] == ["message"]
    assert sent["params"]["body"]["sort"] == [{"@timestamp": {"order": "desc"}}]
    assert fake.calls[0].headers["kbn-xsrf"] == "true" and fake.calls[0].headers["Authorization"].startswith("Basic")
    assert out["ambiente"] == "homol" and out["total"] == 7
    item = out["itens"][0]
    assert item["app.nome"] == "x" and len(item["message"]) == server.TEXTO_MAX + 1


def test_buscar_logs_limite():
    with pytest.raises(Erro, match="elk_exportar_csv"):
        server.elk_buscar_logs("now-1h", indice="logs-*", consulta="x", limite=500)


def test_contar_agrupado_es_direto(fake_es):
    fake_es.routes[("POST", f"{ES}/logs-*/_search")] = (200, {"hits": {"total": {"value": 30}, "hits": []},
        "aggregations": {"grupos": {"buckets": [{"key": "app-a", "doc_count": 20}]}}})
    out = server.elk_contar("now-1h", indice="logs-*", consulta="x", agrupar_por="sis.nome", top=3)
    assert body(fake_es.calls[0])["aggs"] == {"grupos": {"terms": {"field": "sis.nome", "size": 3}}}
    assert out["total"] == 30 and out["grupos"] == [{"valor": "app-a", "total": 20}]


def test_erro_de_certificado(fake, monkeypatch):
    def ssl_error(req):
        raise requests.exceptions.SSLError("CERTIFICATE_VERIFY_FAILED")

    fake.routes[("POST", f"{KIBANA}/internal/search/es")] = ssl_error
    with pytest.raises(Erro, match="elk-bundle.pem"):
        server.elk_contar("now-1h", indice="logs-*", consulta="x")


def test_http_403(fake):
    fake.routes[("POST", f"{KIBANA}/internal/search/es")] = (403, b'{"error":"Forbidden"}')
    with pytest.raises(server.ElkHttpError, match="HTTP 403 em POST"):
        server.elk_contar("now-1h", indice="logs-*", consulta="x")


# ---------------------------------------------------------------- consultas salvas, índices e campos

def test_listar_consultas(fake):
    search = {"id": "s1", "type": "search", "attributes": {"title": "Erros", "columns": ["message"],
        "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({
            "query": {"query": "level:ERROR", "language": "kuery"},
            "filter": [{"meta": {"negate": True}, "query": {"match_phrase": {"a": "b"}}},
                       {"meta": {"disabled": True}, "query": {"match_phrase": {"c": "d"}}}]})}},
        "references": [{"type": "index-pattern", "id": "dv1"}]}
    saved_query = {"id": "q1", "type": "query", "attributes": {"title": "Lentas", "description": "",
        "query": {"query": "tempo:>1000", "language": "lucene"}, "filters": []}}
    dv = {"id": "dv1", "type": "index-pattern", "attributes": {"title": "logs-*", "timeFieldName": "@timestamp"}}

    def find(req):
        return 200, {"saved_objects": [dv] if "index-pattern" in req.url else [search, saved_query]}

    fake.routes[("GET", f"{KIBANA}/api/spaces/space")] = (200, [{"id": "default"}, {"id": "neg"}])
    fake.routes[("GET", f"{KIBANA}/api/saved_objects/_find")] = find
    fake.routes[("GET", f"{KIBANA}/s/neg/api/saved_objects/_find")] = (200, {"saved_objects": []})
    resp = server.elk_listar_consultas(busca="Err")
    assert "search=Err%2A" in fake.calls[1].url
    out = resp["itens"]
    assert resp["total"] == 2
    # Novo formato: apenas id, espaco, tipo e titulo
    assert out[0] == {"id": "s1", "espaco": "default", "tipo": "search", "titulo": "Erros"}
    assert out[1] == {"id": "q1", "espaco": "default", "tipo": "query", "titulo": "Lentas"}
    assert server.elk_listar_indices(espaco="default")["itens"] == [
        {"espaco": "default", "nome": "logs-*", "padrao": "logs-*", "campo_tempo": "@timestamp"}]
    assert server.elk_listar_consultas(espaco="neg") == {"total": 0, "itens": []}


def test_obter_consulta(fake):
    """Testa elk_obter_consulta que retorna os detalhes completos de uma consulta."""
    search = {"id": "s1", "type": "search", "attributes": {"title": "Erros", "description": "Logs de erro",
        "columns": ["message"], "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({
            "query": {"query": "level:ERROR", "language": "kuery"},
            "filter": [{"meta": {"negate": True}, "query": {"match_phrase": {"a": "b"}}}]})}},
        "references": [{"type": "index-pattern", "id": "dv1"}]}
    dv = {"id": "dv1", "type": "index-pattern", "attributes": {"title": "logs-*", "timeFieldName": "@timestamp"}}

    fake.routes[("GET", f"{KIBANA}/api/spaces/space")] = (200, [{"id": "default"}])
    fake.routes[("GET", f"{KIBANA}/api/saved_objects/search/s1")] = (200, search)
    fake.routes[("GET", f"{KIBANA}/api/saved_objects/_find")] = (200, {"saved_objects": [dv]})

    # Teste sem espaco (busca em todos)
    out = server.elk_obter_consulta("s1")
    assert out["id"] == "s1"
    assert out["titulo"] == "Erros"
    assert out["descricao"] == "Logs de erro"
    assert out["consulta"] == "level:ERROR"
    assert out["linguagem"] == "kuery"
    assert out["filtros"] == [{"bool": {"must_not": [{"match_phrase": {"a": "b"}}]}}]
    assert out["indice"] == "logs-*"
    assert out["colunas"] == ["message"]

    # Teste com espaco específico (mais eficiente)
    out2 = server.elk_obter_consulta("s1", espaco="default")
    assert out2["id"] == "s1"
    assert out2["espaco"] == "default"


def test_obter_consulta_nao_encontrada(fake):
    """Testa erro quando a consulta não existe."""
    fake.routes[("GET", f"{KIBANA}/api/spaces/space")] = (200, [{"id": "default"}])
    fake.routes[("GET", f"{KIBANA}/api/saved_objects/search/inexistente")] = (404, {"error": "Not Found"})
    fake.routes[("GET", f"{KIBANA}/api/saved_objects/query/inexistente")] = (404, {"error": "Not Found"})

    with pytest.raises(LookupError, match="não encontrada"):
        server.elk_obter_consulta("inexistente")


def test_listar_campos(fake):
    fake.routes[("GET", f"{KIBANA}/internal/data_views/_fields_for_wildcard")] = (200, {"fields": [
        {"name": "sis.nome", "esTypes": ["keyword"], "aggregatable": True},
        {"name": "_id", "type": "string"}, {"name": "message", "esTypes": ["text"], "aggregatable": False}]})
    assert server.elk_listar_campos("sis", indice="logs-*") == {"total": 1, "itens": [
        {"campo": "sis.nome", "tipo": "keyword", "agregavel": True}]}


# ---------------------------------------------------------------- exportação CSV

def _hit(i, ts):
    return {"_id": str(i), "_index": "l", "_source": {"ts": ts, "a": {"b": i}, "tags": ["x"]}, "sort": [ts]}


def test_exportar_csv_sem_pit_nao_perde_nem_duplica(fake, tmp_path, monkeypatch):
    monkeypatch.setattr(server, "PAGINA", 3)
    docs = [_hit(1, 10), _hit(2, 20), _hit(3, 20), _hit(4, 20), _hit(5, 30)]

    def search(req):  # simula o ES: ordena por ts e aplica search_after
        b = body(req)["params"]["body"]
        after = b.get("search_after", [float("-inf")])[0]
        page = [d for d in docs if d["sort"][0] > after][:b["size"]]
        return 200, {"rawResponse": {"hits": {"total": {"value": 5}, "hits": page}}}

    fake.routes[("POST", f"{KIBANA}/internal/search/es")] = search
    arquivo = tmp_path / "sub" / "out.csv"
    out = server.elk_exportar_csv("now-1h", arquivo=str(arquivo), indice="l", consulta="x", campo_tempo="ts")
    rows = list(csv.DictReader(arquivo.open(encoding="utf-8-sig")))
    assert [r["a.b"] for r in rows] == ["1", "2", "3", "4", "5"]
    assert rows[0]["tags"] == '["x"]'
    assert out.pop("proximo").startswith("não leia o CSV inteiro")
    assert out == {"arquivo": str(arquivo), "linhas": 5, "total": 5, "truncado": False,
                   "colunas": ["ts", "a.b", "tags"]}


def test_exportar_csv_pit_e_truncado(fake_es, tmp_path, monkeypatch):
    monkeypatch.setattr(server, "PAGINA", 2)
    fake_es.routes[("POST", f"{ES}/l/_pit")] = (200, {"id": "pit1"})
    fake_es.routes[("DELETE", f"{ES}/_pit")] = (200, {})
    docs = [_hit(i, i) for i in range(1, 6)]

    def search(req):
        b = body(req)
        assert b["pit"]["id"] == "pit1" and b["sort"][1] == {"_shard_doc": "asc"}
        after = b.get("search_after", [0])[0]
        return 200, {"pit_id": "pit1", "hits": {"total": {"value": 5}, "hits": [d for d in docs if d["sort"][0] > after][:2]}}

    fake_es.routes[("POST", f"{ES}/_search")] = search
    out = server.elk_exportar_csv("now-1h", arquivo=str(tmp_path / "o.csv"), indice="l", consulta="x",
                                  retornar=["a.b"], max_linhas=3, campo_tempo="ts")
    assert out["linhas"] == 3 and out["truncado"] is True and out["colunas"] == ["a.b"]
    assert fake_es.calls[-1].method == "DELETE"


def test_exportar_csv_caminho_relativo():
    with pytest.raises(Erro, match="absoluto"):
        server.elk_exportar_csv("now-1h", arquivo="out.csv", indice="l", consulta="x")


def test_filtros_kibana_ignora_vazio():
    assert server._filtros_kibana([{"meta": {}, "query": {}}, {"meta": {"negate": True}, "query": {"term": {"a": 1}}}]) == [
        {"bool": {"must_not": [{"term": {"a": 1}}]}}]


def test_criar_consulta_search_com_data_view(fake):
    dv = {"id": "dv1", "type": "index-pattern", "attributes": {"title": "logs-*"}}
    fake.routes[("GET", f"{KIBANA}/s/neg/api/saved_objects/_find")] = (200, {"saved_objects": [dv]})
    fake.routes[("POST", f"{KIBANA}/s/neg/api/saved_objects/search")] = (200, {"id": "novo"})
    out = server.elk_criar_consulta("Erros", indice="logs-*", consulta="msg: *timeout*", campos={"sis": "x", "cat": "a*"},
                                    filtros=[{"term": {"a": 1}}], colunas=["message"], espaco="neg")
    assert out == {"id": "novo", "titulo": "Erros", "espaco": "neg"}
    sent = body(fake.calls[-1])
    assert [r["name"] for r in sent["references"]] == ["kibanaSavedObjectMeta.searchSourceJSON.index"] + [
        f"kibanaSavedObjectMeta.searchSourceJSON.filter[{i}].meta.index" for i in range(3)]
    assert {r["id"] for r in sent["references"]} == {"dv1"}
    src = json.loads(sent["attributes"]["kibanaSavedObjectMeta"]["searchSourceJSON"])
    assert src["query"] == {"query": "msg: *timeout*", "language": "kuery"}
    pilula = src["filter"][0]["meta"]
    assert (pilula["type"], pilula["key"], pilula["params"]) == ("phrase", "sis", {"query": "x"})
    assert server._filtros_kibana(src["filter"]) == [{"match_phrase": {"sis": "x"}},
                                                     {"wildcard": {"cat": {"value": "a*"}}}, {"term": {"a": 1}}]
    assert sent["attributes"]["columns"] == ["message"]
    server.elk_criar_consulta("L", indice="logs-*", consulta="n:[1 TO 5]", linguagem="lucene", espaco="neg")
    assert json.loads(body(fake.calls[-1])["attributes"]["kibanaSavedObjectMeta"]["searchSourceJSON"])["query"][
        "language"] == "lucene"
    with pytest.raises(Erro, match="Linguagem desconhecida"):
        server.elk_criar_consulta("X", indice="logs-*", consulta="a", linguagem="sql", espaco="neg")
    with pytest.raises(LookupError, match="Data view 'outro-\\*'"):
        server.elk_criar_consulta("X", indice="outro-*", consulta="a", espaco="neg")


def test_atualizar_consulta_preserva_o_resto(fake):
    url = f"{KIBANA}/s/neg/api/saved_objects/search/s1"
    src = {"query": {"query": "msg: x", "language": "kuery"},
           "filter": [{"meta": {"indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.filter[0].meta.index"},
                       "query": {"match_phrase": {"a": "b"}}}], "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"}
    refs = [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern", "id": "dv1"},
            {"name": "kibanaSavedObjectMeta.searchSourceJSON.filter[0].meta.index", "type": "index-pattern", "id": "dv1"}]
    fake.routes[("GET", url)] = (200, {"id": "s1", "type": "search", "references": refs, "attributes": {
        "title": "Velho", "description": "d", "columns": ["m"], "sort": [["@timestamp", "desc"]],
        "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps(src)}}})
    fake.routes[("PUT", url)] = (200, {"id": "s1"})

    assert server.elk_atualizar_consulta("s1", espaco="neg", titulo="Novo") == {"id": "s1", "titulo": "Novo", "espaco": "neg"}
    sent = body(fake.calls[-1])
    assert sent["attributes"]["sort"] == [["@timestamp", "desc"]] and sent["attributes"]["columns"] == ["m"]
    assert sent["references"] == refs
    novo = json.loads(sent["attributes"]["kibanaSavedObjectMeta"]["searchSourceJSON"])
    assert novo["query"] == src["query"] and novo["filter"] == src["filter"]

    server.elk_atualizar_consulta("s1", espaco="neg", campos={"sis": "x", "nb": 0}, consulta="")
    sent = body(fake.calls[-1])
    novo = json.loads(sent["attributes"]["kibanaSavedObjectMeta"]["searchSourceJSON"])
    assert novo["query"] == {"query": "", "language": "kuery"}
    assert server._filtros_kibana(novo["filter"]) == [{"match_phrase": {"sis": "x"}}, {"match_phrase": {"nb": 0}}]
    assert len(sent["references"]) == 3 and {r["id"] for r in sent["references"]} == {"dv1"}

    with pytest.raises(Erro, match="Nada a atualizar"):
        server.elk_atualizar_consulta("s1", espaco="neg")
    with pytest.raises(LookupError, match="não encontrada"):
        server.elk_atualizar_consulta("zz", espaco="neg", titulo="x")


# ---------------------------------------------------------------- contrato e tools/list

def test_tools_list_enxuto_sem_output_schema():
    import asyncio
    tools = asyncio.run(server.mcp.list_tools())
    assert len(tools) == 12
    assert sum(len(t.description or "") + len(json.dumps(t.input_schema)) for t in tools) <= 11_500  # medido: 10_760
    for t in tools:
        assert t.output_schema is None and "→" in t.description, t.name


def test_envelope():
    from mcp_elk.contrato import operacao

    def falha():
        raise Erro("config", "sem arquivo", "indice")

    assert json.loads(operacao(falha)()) == {"ok": False, "erro": {"codigo": "config", "detalhe": "sem arquivo",
                                                                    "campo": "indice"}}
    assert operacao(lambda: {"a": 1})() == '{"ok":true,"dados":{"a":1}}'
    assert operacao(lambda: "# md")() == "# md"


# ---------------------------------------------------------------- config do projeto

CONFIG = {"indice-padrao": "app", "indices": {"app": {
    "padrao": "logs-*", "campo-tempo": "ts", "fixos": {"sis": "x*"},
    "campos": {"nivel": "log.level", "mensagem": "message", "logger": "log.logger", "thread": "proc.thread"}}},
    "consultas": {"Por NB": {"id": "s1"}}, "download": {"path": "downloads/elk"}}


@pytest.fixture
def projeto(tmp_path):
    server.elk_configurar(str(tmp_path), CONFIG)
    return str(tmp_path)


def test_configurar_valida_e_grava(tmp_path):
    with pytest.raises(Erro, match="indices.app.padrao"):
        server.elk_configurar(str(tmp_path), {"indice-padrao": "app", "indices": {"app": {"campo-tempo": "ts"}}})
    with pytest.raises(Erro, match="indice-padrao"):
        server.elk_configurar(str(tmp_path), {**CONFIG, "indice-padrao": "outro"})
    out = server.elk_configurar(str(tmp_path), CONFIG, ambiente="hml")
    assert out == {"arquivo": ".kiro/config/elk/elk-homol.json"}
    gravado = json.loads((tmp_path / out["arquivo"]).read_text("utf-8"))
    assert list(gravado)[0] == "$schema" and gravado["indices"] == CONFIG["indices"]


def test_config_aplica_fixos_apelidos_e_parametros(fake, projeto):
    fake.routes[("POST", f"{KIBANA}/internal/search/es")] = (200, {"rawResponse": {"hits": {"total": 3, "hits": []}}})
    out = server.elk_contar("now-1h", projeto=projeto, campos={"nivel": "ERROR"}, consulta="mensagem:*timeout* and n > 0")
    sent = body(fake.calls[-1])["params"]
    assert sent["index"] == "logs-*" and out["total"] == 3
    assert sent["body"]["query"] == {"bool": {
        "must": [{"query_string": {"query": "message:*timeout* AND n:>0"}}],
        "filter": [{"wildcard": {"sis": {"value": "x*"}}}, {"term": {"log.level": "ERROR"}},
                   {"range": {"ts": {"gte": "now-1h", "lte": "now"}}}]}}

    salva = {"id": "s1", "type": "search", "attributes": {"title": "Por NB", "description": "Parâmetros: nb",
        "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({"query": {"query": "a: 1 and b: 2", "language": "kuery"},
            "filter": [{"meta": {}, "query": {"match_phrase": {"nb": 0}}}, {"meta": {}, "query": {"term": {"c": 1}}}]})}},
        "references": []}
    fake.routes[("GET", f"{KIBANA}/api/spaces/space")] = (200, [{"id": "default"}])
    fake.routes[("GET", f"{KIBANA}/api/saved_objects/search/s1")] = (200, salva)
    with pytest.raises(Erro, match="exige parâmetro"):
        server.elk_contar("now-1h", projeto=projeto, salva="por nb")
    server.elk_contar("now-1h", projeto=projeto, salva="Por NB", campos={"nb": 123})
    q = body(fake.calls[-1])["params"]["body"]["query"]["bool"]
    assert q["must"] == [{"query_string": {"query": "(a: 1 AND b: 2)"}}]
    assert q["filter"][:3] == [{"wildcard": {"sis": {"value": "x*"}}}, {"term": {"nb": 123}}, {"term": {"c": 1}}]


def test_sem_config_exige_indice(fake, tmp_path):
    with pytest.raises(Erro, match="elk_configurar"):
        server.elk_contar("now-1h", projeto=str(tmp_path), consulta="x")


def test_exportar_por_assunto(fake, projeto, monkeypatch):
    fake.routes[("POST", f"{KIBANA}/internal/search/es")] = (200, {"rawResponse": {"hits": {"total": 1, "hits": [
        _hit(1, 10)]}}})
    out = server.elk_exportar_csv("now-1h", projeto=projeto, assunto="erros-x", consulta="x", retornar=["mensagem"])
    assert out["arquivo"].startswith("downloads/elk/") and out["arquivo"].endswith("_erros-x.csv")
    assert out["colunas"] == ["message"]


def test_contar_comparar(fake, projeto):
    def search(req):
        b = body(req)["params"]["body"]
        ref = "-1d" in b["query"]["bool"]["filter"][-1]["range"]["ts"]["gte"]
        buckets = [{"key": "ERROR", "doc_count": 5}] if ref else [{"key": "ERROR", "doc_count": 50},
                                                                   {"key": "FATAL", "doc_count": 2}]
        return 200, {"rawResponse": {"hits": {"total": 10 if ref else 60}, "aggregations": {"grupos": {"buckets": buckets}}}}

    fake.routes[("POST", f"{KIBANA}/internal/search/es")] = search
    out = server.elk_contar("now-1h", projeto=projeto, campos={"nivel": "*"}, agrupar_por="nivel", comparar="1d")
    assert out["referencia"] == {"inicio": "now-1h-1d", "fim": "now-1d", "total": 10}
    assert out["grupos"] == [{"valor": "ERROR", "total": 50, "ref": 5, "novo": False},
                             {"valor": "FATAL", "total": 2, "ref": 0, "novo": True}]


# ---------------------------------------------------------------- job Spring Batch

def _linhas_job():
    L = "br.app.StepLoggerListener"
    ev = [("main", "br.app.JobLoggerListener", "INFO", "Iniciando processamento do JOB: carga na data 09/10/2026 11:43:42"),
          ("main", L, "INFO", "Iniciando step: prepara na data 09/10/2026 11:43:42"),
          ("main", L, "INFO", "##### SUMÁRIO DE EXECUCAO #####"),
          ("main", L, "INFO", "Situação do step:                COMPLETED"),
          ("main", L, "INFO", "Total de linhas lidas:           7"),
          ("main", L, "INFO", "Finalizando step: prepara na data 09/10/2026 11:43:43 com duração de 00:00:01"),
          ("main", L, "INFO", "Iniciando step: grava na data 09/10/2026 11:43:43")]
    for p in (0, 1):
        ev.append((f"exec-{p}", L, "INFO", f"Iniciando step: grava:partition{p} na data 09/10/2026 11:43:44"))
    for p in (0, 1):  # sumários intercalados entre threads
        ev.append((f"exec-{p}", L, "INFO", "##### SUMÁRIO DE EXECUCAO #####"))
    for p in (0, 1):
        ev.append((f"exec-{p}", L, "INFO", "Situação do step:                COMPLETED"))
        ev.append((f"exec-{p}", L, "INFO", f"Total de linhas lidas:           {10 + p}"))
        ev.append((f"exec-{p}", L, "INFO", "Total de rollback:               0"))
    ev.append(("exec-1", "br.app.Repo", "ERROR", "Timeout 123 no banco"))
    ev.append(("exec-0", "br.app.Repo", "ERROR", "Timeout 456 no banco"))
    for p in (0, 1):
        ev.append((f"exec-{p}", L, "INFO", f"Finalizando step: grava:partition{p} na data 09/10/2026 11:44:00 com duração de 00:00:16"))
    ev += [("main", L, "INFO", "##### SUMÁRIO DE EXECUCAO #####"), ("main", L, "INFO", "Total de linhas lidas:           21"),
           ("main", L, "INFO", "Finalizando step: grava na data 09/10/2026 11:44:01 com duração de 00:00:18"),
           ("main", "br.app.JobLoggerListener", "INFO",
            "Finalizando processamento do JOB: carga na data 09/10/2026 11:44:32, com status COMPLETED")]
    return [{"@timestamp": f"2026-10-09T14:{43 + i // 60:02d}:{i % 60:02d}.000Z", "thread_name": t, "logger_name": lg,
             "level": lv, "message": m} for i, (t, lg, lv, m) in enumerate(ev)]


def test_job_local_consolida_particoes(tmp_path):
    log = tmp_path / "app.log"
    antes = {"@timestamp": "2026-10-09T10:00:00Z", "thread_name": "main", "logger_name": "x.JobLoggerListener",
             "level": "INFO", "message": "Iniciando processamento do JOB: outro na data 09/10/2026 07:00:00"}
    log.write_text("\n".join(json.dumps(e, ensure_ascii=False) for e in [antes, *_linhas_job()]), "utf-8")
    md = server.elk_job(job="carga", log_local=str(log))
    assert "# Execução — carga (local)" in md and "✅ COMPLETED" in md and "Duração: 0:00:50" in md
    assert "| grava | ? | 21 |" in md and "| ↳ partition0 | COMPLETED | 10 |" in md
    assert "| ↳ partition1 | COMPLETED | 11 |" in md and "| ↳ **Σ 2 partições** | COMPLETED | 21 |" in md  # = manager
    assert "⚠️" not in md.split("## WARN")[0] and md.index("| prepara |") < md.index("| grava |")
    assert "| ERROR | Repo | 2× |" in md and "| grava |" in md.split("WARN / ERROR")[1]
    assert server.elk_job(log_local=str(log)).startswith("# Execução — carga")  # último job executado


def test_job_particoes_embaralhadas_no_mesmo_ms():
    """ELK: cada linha do sumário é um documento e as do mesmo ms chegam em qualquer ordem; o worker tem outro nome."""
    L, ts = "x.StepLoggerListener", "2026-10-23T10:31:55.490Z"

    def sumario(thread, n, t, commits=1):
        return [{"ts": t, "thread": thread, "logger": L, "level": "", "msg": m} for m in (
            f"Total de linhas gravadas:        {n}", "Situação do step:                COMPLETED",
            "##### SUMÁRIO DE EXECUCAO #####", f"Total de linhas lidas:           {n}", f"Total de commits:   {commits}")]
    ev = [{"ts": "2026-10-23T10:31:54.000Z", "thread": "main", "logger": L, "level": "",
           "msg": "Iniciando processamento do JOB: exp na data 23/10/2026 07:31:54"}]
    ev += [{"ts": ts, "thread": "main", "logger": L, "level": "", "msg": f"Iniciando step: {n} na data 23/10/2026 07:31:54"}
           for n in ("prep", "expStep")]
    ev += sumario("main", 1, ts)[:2]  # sumário do prep partido em volta do início do manager
    ev.insert(2, sumario("main", 1, ts)[3])
    ev += sumario("main", 1, ts)[2:3] + sumario("main", 1, ts)[4:]
    for p in range(3):
        ev.append({"ts": "2026-10-23T10:31:56.000Z", "thread": f"exec-{p}", "logger": L, "level": "",
                   "msg": f"Iniciando step: expWorker:partition{p} na data 23/10/2026 07:31:56"})
    for p in range(3):
        ev += sumario(f"exec-{p}", 100 + p, "2026-10-23T10:33:50.000Z")[::-1]
    ev += sumario("main", 303, "2026-10-23T10:33:54.000Z", commits=3)
    md = jb.relatorio(ev, None, "prod")
    assert "| prep | COMPLETED | 1 |" in md and "| expStep | COMPLETED | 303 |" in md
    assert "| ↳ partition2 | COMPLETED | 102 |" in md and "| ↳ **Σ 3 partições** | COMPLETED | 303 |" in md
    assert "expWorker" not in md and "2 steps · lidas 304 · gravadas 304" in md


def test_job_elk(fake, projeto):
    eventos = _linhas_job()

    def hit(i, e):
        return {"_id": str(i), "_index": "l", "sort": [i], "_source": {
            "ts": e["@timestamp"], "message": e["message"], "log": {"logger": e["logger_name"], "level": e["level"]},
            "proc": {"thread": e["thread_name"]}}}

    def search(req):
        b = body(req)["params"]["body"]
        q = b["query"]["bool"]["must"][0]["query_string"]["query"]
        if "processamento" in q:
            hits = [hit(i, e) for i, e in enumerate(eventos) if "processamento do JOB" in e["message"]][::-1]
        elif "LoggerListener" in q:
            hits = [hit(i, e) for i, e in enumerate(eventos) if "LoggerListener" in e["logger_name"]]
        else:
            hits = [hit(i, e) for i, e in enumerate(eventos) if e["level"] == "ERROR"]
        return 200, {"rawResponse": {"hits": {"total": len(hits), "hits": hits}}}

    fake.routes[("POST", f"{KIBANA}/internal/search/es")] = search
    md = server.elk_job(projeto=projeto, job="carga")
    assert "(prod)" in md and "| ↳ **Σ 2 partições** | COMPLETED | 21 |" in md and "| ERROR | Repo | 2× |" in md
    janela = body(fake.calls[1])["params"]["body"]["query"]["bool"]["filter"][-1]["range"]["ts"]
    assert janela["gte"].endswith("||-5s") and janela["lte"].endswith("||+5s")
    csvs = list(Path(projeto).rglob("*_job-carga.csv"))
    assert len(csvs) == 1 and csvs[0].relative_to(projeto).as_posix() in md  # baixado e citado no relatório
    refeito = server.elk_job(log_local=str(csvs[0]), erros=False)
    assert refeito.split("## Steps")[1] == md.split("## Steps")[1].split("## WARN")[0].rstrip("\n")
