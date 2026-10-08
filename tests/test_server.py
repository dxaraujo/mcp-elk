import csv
import json
from pathlib import Path

import pytest
import requests
from requests.adapters import BaseAdapter

from mcp_elk import config, server
from mcp_elk.config import ElkConfig

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
    with pytest.raises(ValueError, match="desconhecido"):
        server._ambiente("qa")
    with pytest.raises(ValueError, match="'dev' sem URL"):
        server.elk_contar("logs-*", "now-1h", consulta="x", ambiente="dev")


def test_lucene():
    assert server._lucene('a: x and b: "y and z" or not c') == 'a: x AND b: "y and z" OR NOT c'
    assert server._lucene("android:1 AND brand:x") == "android:1 AND brand:x"


def test_query():
    q = server._query("now-1h", "now", "msg:erro", {"sis.nome": "a", "sis.cat": "b"}, [{"exists": {"field": "f"}}],
                      "ts")
    assert q == {"bool": {"must": [{"query_string": {"query": "msg:erro"}}], "filter": [
        {"term": {"sis.nome": "a"}}, {"term": {"sis.cat": "b"}}, {"exists": {"field": "f"}},
        {"range": {"ts": {"gte": "now-1h", "lte": "now"}}}]}}
    with pytest.raises(ValueError, match="vazia"):
        server._query("now-1h", "now", None, None, None, "ts")


# ---------------------------------------------------------------- busca e contagem

def test_buscar_logs_kibana(fake):
    fake.routes[("POST", f"{KIBANA}/internal/search/es")] = (200, {"rawResponse": {
        "hits": {"total": {"value": 7}, "hits": [HIT]}}})
    out = server.elk_buscar_logs("logs-*", "now-15m", campos={"sis": "a"}, limite=5, retornar=["message"],
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
    with pytest.raises(ValueError, match="elk_exportar_csv"):
        server.elk_buscar_logs("logs-*", "now-1h", consulta="x", limite=500)


def test_contar_agrupado_es_direto(fake_es):
    fake_es.routes[("POST", f"{ES}/logs-*/_search")] = (200, {"hits": {"total": {"value": 30}, "hits": []},
        "aggregations": {"grupos": {"buckets": [{"key": "app-a", "doc_count": 20}]}}})
    out = server.elk_contar("logs-*", "now-1h", consulta="x", agrupar_por="sis.nome", top=3)
    assert body(fake_es.calls[0])["aggs"] == {"grupos": {"terms": {"field": "sis.nome", "size": 3}}}
    assert out["total"] == 30 and out["grupos"] == [{"valor": "app-a", "total": 20}]


def test_erro_de_certificado(fake, monkeypatch):
    def ssl_error(req):
        raise requests.exceptions.SSLError("CERTIFICATE_VERIFY_FAILED")

    fake.routes[("POST", f"{KIBANA}/internal/search/es")] = ssl_error
    with pytest.raises(RuntimeError, match="elk-bundle.pem"):
        server.elk_contar("logs-*", "now-1h", consulta="x")


def test_http_403(fake):
    fake.routes[("POST", f"{KIBANA}/internal/search/es")] = (403, b'{"error":"Forbidden"}')
    with pytest.raises(server.ElkHttpError, match="HTTP 403 em POST"):
        server.elk_contar("logs-*", "now-1h", consulta="x")


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
    assert out[0] == {"id": "s1", "espaco": "default", "tipo": "search", "titulo": "Erros", "descricao": None, "consulta": "level:ERROR",
                      "linguagem": "kuery", "filtros": [{"bool": {"must_not": [{"match_phrase": {"a": "b"}}]}}],
                      "indice": "logs-*", "colunas": ["message"]}
    assert out[1]["tipo"] == "query" and out[1]["consulta"] == "tempo:>1000" and out[1]["indice"] is None
    assert server.elk_listar_indices(espaco="default") == [{"espaco": "default", "nome": "logs-*", "padrao": "logs-*",
                                                            "campo_tempo": "@timestamp"}]
    assert server.elk_listar_consultas(espaco="neg") == {"total": 0, "itens": []}


def test_listar_campos(fake):
    fake.routes[("GET", f"{KIBANA}/internal/data_views/_fields_for_wildcard")] = (200, {"fields": [
        {"name": "sis.nome", "esTypes": ["keyword"], "aggregatable": True},
        {"name": "_id", "type": "string"}, {"name": "message", "esTypes": ["text"], "aggregatable": False}]})
    assert server.elk_listar_campos("logs-*", busca="sis") == [{"campo": "sis.nome", "tipo": "keyword",
                                                                "agregavel": True}]


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
    out = server.elk_exportar_csv("l", "now-1h", str(arquivo), consulta="x", campo_tempo="ts")
    rows = list(csv.DictReader(arquivo.open(encoding="utf-8-sig")))
    assert [r["a.b"] for r in rows] == ["1", "2", "3", "4", "5"]
    assert rows[0]["tags"] == '["x"]'
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
    out = server.elk_exportar_csv("l", "now-1h", str(tmp_path / "o.csv"), consulta="x", retornar=["a.b"],
                                  max_linhas=3, campo_tempo="ts")
    assert out["linhas"] == 3 and out["truncado"] is True and out["colunas"] == ["a.b"]
    assert fake_es.calls[-1].method == "DELETE"


def test_exportar_csv_caminho_relativo():
    with pytest.raises(ValueError, match="absoluto"):
        server.elk_exportar_csv("l", "now-1h", "out.csv", consulta="x")
