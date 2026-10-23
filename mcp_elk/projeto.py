"""Arquivo do projeto: <projeto>/.kiro/config/elk/elk-<ambiente>.json, um por ambiente, validado pelo elk.schema.json.

O caminho do projeto vem em cada chamada: o cliente não inicia o MCP no workspace.
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path

import jsonschema

from .contrato import Erro

PASTA = Path(".kiro") / "config" / "elk"
SCHEMA_URL = "https://raw.githubusercontent.com/dxaraujo/kiro/main/mcps/elk/mcp_elk/elk.schema.json"
_VALIDADOR = jsonschema.Draft202012Validator(json.loads(Path(__file__).with_name("elk.schema.json").read_text("utf-8")))
_cache: dict[Path, tuple[int, dict]] = {}


def raiz(projeto: str) -> Path:
    if not os.path.isabs(projeto):
        raise Erro("entrada", "projeto precisa ser o caminho absoluto do workspace", "projeto")
    if not os.path.isdir(projeto):
        raise Erro("entrada", f"pasta do projeto não existe: {projeto}", "projeto")
    return Path(projeto)


def arquivo(projeto: str, amb: str) -> Path:
    return raiz(projeto) / PASTA / f"elk-{amb}.json"


def faltando(dados) -> list[str]:
    """Campos ausentes ou inválidos segundo o schema, em caminho com ponto: ['indices.app.padrao']."""
    out = set()
    for e in _VALIDADOR.iter_errors(dados):
        path = ".".join(str(p) for p in e.absolute_path)
        if e.validator in ("required", "additionalProperties") and (m := re.search(r"'([^']+)'", e.message)):
            path = f"{path}.{m.group(1)}" if path else m.group(1)
        out.add(path or "config")
    if isinstance(dados, dict) and dados.get("indice-padrao") not in (dados.get("indices") or {}):
        out.add("indice-padrao")
    return sorted(out)


def carregar(projeto: str, amb: str) -> dict | None:
    """Config validada do ambiente (cache pelo mtime), ou None se o arquivo não existe (projeto sem setup)."""
    arq = arquivo(projeto, amb)
    if not arq.is_file():
        return None
    mtime = arq.stat().st_mtime_ns
    hit = _cache.get(arq)
    if hit is None or hit[0] != mtime:
        try:
            dados = json.loads(arq.read_text("utf-8"))
        except ValueError as exc:
            raise Erro("config", f"{arq.name} não é JSON válido: {exc}") from exc
        if erros := faltando(dados):
            raise Erro("config", f"{arq.name}: ausente ou inválido: {', '.join(erros)} (corrija com elk_configurar)",
                       erros[0])
        hit = _cache[arq] = (mtime, dados)
    return hit[1]


def gravar(projeto: str, amb: str, dados: dict) -> Path:
    """Valida e grava (indent=2, `$schema` primeiro). Erro `entrada` com os campos inválidos, sem gravar nada."""
    dados = {"$schema": SCHEMA_URL, **{k: v for k, v in dados.items() if k != "$schema"}}
    if erros := faltando(dados):
        raise Erro("entrada", f"config inválida: {', '.join(erros)}", erros[0])
    arq = arquivo(projeto, amb)
    arq.parent.mkdir(parents=True, exist_ok=True)
    arq.write_text(json.dumps(dados, indent=2, ensure_ascii=False) + "\n", "utf-8")
    return arq
