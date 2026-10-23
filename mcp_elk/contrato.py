"""Contrato das tools: envelope JSON compacto {ok, dados} | {ok, erro{codigo, detalhe, campo?}}; Markdown passa direto."""
from __future__ import annotations

import functools
import json

import requests

CODIGOS = ("config", "credencial", "certificado", "indisponivel", "nao_encontrado", "permissao", "entrada")


class Erro(Exception):
    def __init__(self, codigo: str, detalhe: str, campo: str | None = None):
        assert codigo in CODIGOS, codigo
        super().__init__(detalhe)
        self.codigo, self.detalhe, self.campo = codigo, detalhe, campo


class ElkHttpError(RuntimeError):
    def __init__(self, resp: requests.Response):
        super().__init__(f"HTTP {resp.status_code} em {resp.request.method} {resp.url}: {resp.text[:500].strip()}")
        self.status_code = resp.status_code


def _json(obj) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False, default=str)


def erro(exc: Exception) -> Erro | None:
    """Exceção -> Erro do contrato; None se for falha inesperada (bug: sobe como erro do MCP)."""
    if isinstance(exc, Erro):
        return exc
    if isinstance(exc, ElkHttpError):
        codigo = {401: "credencial", 403: "permissao", 404: "nao_encontrado"}.get(exc.status_code, "entrada")
        if exc.status_code >= 500:
            codigo = "indisponivel"
        return Erro(codigo, str(exc))
    if isinstance(exc, requests.Timeout):
        return Erro("indisponivel", f"servidor sem resposta no prazo (reduza a janela pela metade): {exc}"[:300])
    if isinstance(exc, requests.ConnectionError):
        return Erro("indisponivel", f"servidor inacessível (rede, VPN ou URL do elk.properties): {exc}"[:300])
    if isinstance(exc, FileNotFoundError):
        return Erro("config", str(exc))
    if isinstance(exc, LookupError):
        return Erro("nao_encontrado", str(exc))
    if isinstance(exc, (ValueError, OSError, RuntimeError)):
        return Erro("entrada", str(exc))
    return None


def operacao(fn):
    """Tool -> envelope. Retorno str (Markdown) passa direto; o resto vira {"ok":true,"dados":...}."""
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            result = fn(*args, **kwargs)
        except Exception as exc:
            if (e := erro(exc)) is None:
                raise
            return _json({"ok": False, "erro": {"codigo": e.codigo, "detalhe": e.detalhe,
                                                **({"campo": e.campo} if e.campo else {})}})
        return result if isinstance(result, str) else _json({"ok": True, "dados": result})
    return wrapper
