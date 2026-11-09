"""Carrega credenciais, servidores e certificado do ELK das variáveis de ambiente."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

REQUIRED = ("ELK_USER", "ELK_PASSWORD", "ELK_URL_PROD")
AMBIENTES = ("prod", "homol", "dev")


@dataclass(frozen=True)
class ElkConfig:
    user: str
    password: str = field(repr=False)
    kibana: dict = field(default_factory=dict)  # {ambiente: url do Kibana}
    es: dict = field(default_factory=dict)  # {ambiente: url do Elasticsearch}, opcional (ES_URL_<AMBIENTE>)


def ca_bundle() -> Path | None:
    """Cadeia PEM extra apontada por ELK_CA_BUNDLE, somada às CAs da máquina."""
    valor = os.environ.get("ELK_CA_BUNDLE", "").strip()
    if not valor:
        return None
    path = Path(valor).expanduser()
    if not path.is_file():
        raise ValueError(f"ELK_CA_BUNDLE aponta para um arquivo que não existe: {path}")
    return path


def load_config() -> ElkConfig:
    missing = [k for k in REQUIRED if not os.environ.get(k, "").strip()]
    if missing:
        raise ValueError(f"Variável(is) de ambiente ausente(s): {', '.join(missing)}")
    ca_bundle()  # arquivo errado aparece já na carga

    def urls(prefix: str) -> dict:
        return {a: v.strip().rstrip("/") for a in AMBIENTES if (v := os.environ.get(prefix + a.upper(), "")).strip()}

    return ElkConfig(user=os.environ["ELK_USER"].strip(), password=os.environ["ELK_PASSWORD"],
                     kibana=urls("ELK_URL_"), es=urls("ES_URL_"))
