"""Carrega credenciais e servidores do ELK a partir de elk.properties."""
from __future__ import annotations

import configparser
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

REQUIRED = ("user", "password", "prod")
AMBIENTES = ("prod", "homol", "dev")
TEMPLATE = (
    "[DEFAULT]\nuser = SEU_USUARIO\npassword = SUA_SENHA\n"
    "prod = https://logs.SEU-SERVIDOR\nhomol = https://hlogs.SEU-SERVIDOR\ndev = https://dlogs.SEU-SERVIDOR"
)


@dataclass(frozen=True)
class ElkConfig:
    user: str
    password: str = field(repr=False)
    kibana: dict = field(default_factory=dict)  # {ambiente: url do Kibana}
    es: dict = field(default_factory=dict)  # {ambiente: url do Elasticsearch}, opcional (es_<ambiente>)


def default_path() -> Path:
    if env := os.environ.get("MCP_ELK_CONFIG"):
        return Path(env)
    if sys.platform == "win32":
        return Path(os.environ["APPDATA"]) / "mcp-elk" / "elk.properties"
    return Path.home() / ".config" / "mcp-elk" / "elk.properties"


def load_config(path: Path | None = None) -> ElkConfig:
    path = path or default_path()
    if not path.is_file():
        raise FileNotFoundError(f"Arquivo de credenciais não encontrado: {path}\nCrie-o com:\n{TEMPLATE}")
    # interpolation=None: senhas podem conter '%'
    parser = configparser.ConfigParser(interpolation=None)
    try:
        parser.read(path, encoding="utf-8")
    except configparser.Error as exc:  # ex.: chave repetida
        raise ValueError(f"{path} inválido: {exc}") from exc
    section = parser.defaults()
    missing = [k for k in REQUIRED if not section.get(k, "").strip()]
    if missing:
        raise ValueError(f"Chave(s) ausente(s) em {path} [DEFAULT]: {', '.join(missing)}")

    def urls(prefix: str) -> dict:
        return {a: v.strip().rstrip("/") for a in AMBIENTES if (v := section.get(prefix + a, "")).strip()}

    return ElkConfig(user=section["user"].strip(), password=section["password"], kibana=urls(""), es=urls("es_"))
