"""Execução de job Spring Batch a partir das linhas dos listeners de job e de step ("...LoggerListener"):
sumário por step (partições consolidadas pela thread) e WARN/ERROR agrupados, em Markdown.

Evento = {ts, thread, logger, msg, level}; `ts` só ordena e recorta (no ELK é UTC); horários exibidos vêm da mensagem.
"""
from __future__ import annotations

import json
import re
import unicodedata
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

BRASILIA = timezone(timedelta(hours=-3))
INICIO_JOB = re.compile(r"Iniciando processamento do JOB:\s*(\S+)\s+na data\s+(.+?)\s*$")
FIM_JOB = re.compile(r"Finalizando processamento do JOB:\s*(\S+)\s+na data\s+(.+?),?\s+com status\s+(\w+)")
DURACAO_JOB = re.compile(r"JOB:\s*(\S+)\s+executado com dura\S+o:?\s*(\d+:\d\d:\d\d)")
INICIO_STEP = re.compile(r"Iniciando step:\s*(\S+)\s+na data\s+(.+?)\s*$")
FIM_STEP = re.compile(r"Finalizando step:\s*(\S+)\s+na data\s+(.+?)\s+com dura\S+o de\s+(\d+:\d\d:\d\d)")
SUMARIO = re.compile(r"SUM.RIO DE EXECU", re.I)
SITUACAO = re.compile(r"Situa\S+o do step:\s*(\w+)")
TOTAL = re.compile(r"Total de ([^:]+?):\s*(\d+)")
PARTICAO = re.compile(r"^(.+?):partition\d+$")
CONTADORES = {"linhas lidas": "lidas", "skips na leitura": "skip_leitura", "linhas filtradas": "filtradas",
              "linhas gravadas": "gravadas", "skips na gravacao": "skip_gravacao",
              "skips no processamento": "skip_processamento", "commits": "commits", "rollback": "rollback"}
# texto do Spring Boot: "2026-10-09T11:43:42.736-03:00  INFO 9266 --- [main] a.b.Classe : mensagem"
_LINHA_TEXTO = re.compile(r"^(\S+)\s+([A-Z]+)\s+\d+\s+---\s+\[\s*(.*?)\]\s+(\S+)\s*:\s?(.*)$")


def _sem_acento(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower().strip()


def ler_local(caminho: str) -> list[dict]:
    """Eventos de um log local: JSONL (preferido: message, logger_name, thread_name, level, @timestamp) ou texto do
    Spring Boot (linhas sem prefixo continuam a mensagem anterior)."""
    path = Path(caminho)
    if not path.is_absolute() or not path.is_file():
        raise ValueError(f"log_local precisa ser o caminho absoluto de um arquivo existente: {caminho}")
    eventos = []
    with path.open(encoding="utf-8", errors="replace") as f:
        for linha in f:
            if linha.startswith("{"):
                try:
                    d = json.loads(linha)
                except ValueError:
                    continue
                eventos.append({"ts": d.get("@timestamp") or d.get("timestamp", ""), "thread": d.get("thread_name", ""),
                                "logger": d.get("logger_name", ""), "msg": d.get("message", ""),
                                "level": d.get("level", "")})
            elif m := _LINHA_TEXTO.match(linha.rstrip("\n")):
                eventos.append({"ts": m[1], "level": m[2], "thread": m[3], "logger": m[4], "msg": m[5]})
            elif eventos and linha.strip():
                eventos[-1]["msg"] += "\n" + linha.rstrip("\n")
    return eventos


def recortar(eventos: list[dict], job: str | None) -> tuple[list[dict], str | None]:
    """Da última "Iniciando processamento do JOB" (do `job`, ou de qualquer um) até o fim correspondente."""
    inicio, nome = None, None
    for i, e in enumerate(eventos):
        if (m := INICIO_JOB.search(e["msg"])) and (job is None or m[1] == job):
            inicio, nome = i, m[1]
    if inicio is None:
        return [], None
    for j in range(inicio + 1, len(eventos)):
        if (m := FIM_JOB.search(eventos[j]["msg"])) and m[1] == nome:
            return eventos[inicio:j + 1], nome
    return eventos[inicio:], nome


def _dt(texto: str) -> datetime | None:
    try:
        return datetime.strptime(texto.strip(), "%d/%m/%Y %H:%M:%S")
    except ValueError:
        return None


def _steps(eventos: list[dict]) -> tuple[dict, dict]:
    """{step: {inicio, fim, duracao, ts_ini, ts_fim, sumarios[]}} na ordem de início + dados do job."""
    steps: dict[str, dict] = {}
    job: dict = {}
    pendente: dict[str, dict] = {}  # thread -> sumário aberto
    atual: dict[str, str] = {}  # thread -> último step iniciado
    for e in eventos:
        thread = e.get("thread") or ""
        for linha in (e["msg"] or "").splitlines():
            if m := INICIO_JOB.search(linha):
                job.update(nome=m[1], inicio=m[2])
            elif m := FIM_JOB.search(linha):
                job.update(fim=m[2], status=m[3])
            elif m := DURACAO_JOB.search(linha):
                job["duracao"] = m[2]
            elif m := INICIO_STEP.search(linha):
                s = steps.setdefault(m[1], {"sumarios": []})
                s.update(inicio=m[2], ts_ini=e["ts"])
                atual[thread] = m[1]
            elif m := FIM_STEP.search(linha):
                s = steps.setdefault(m[1], {"sumarios": []})
                s.update(fim=m[2], duracao=m[3], ts_fim=e["ts"])
                if thread in pendente:
                    s["sumarios"].append(pendente.pop(thread))
            elif SUMARIO.search(linha):
                if thread in pendente and thread in atual:  # bloco anterior sem "Finalizando step"
                    steps[atual[thread]]["sumarios"].append(pendente.pop(thread))
                pendente[thread] = {}
            elif thread in pendente and (m := SITUACAO.search(linha)):
                pendente[thread]["situacao"] = m[1]
            elif thread in pendente and (m := TOTAL.search(linha)):
                if chave := CONTADORES.get(_sem_acento(m[1])):
                    pendente[thread][chave] = int(m[2])
    for thread, bloco in pendente.items():
        if thread in atual:
            steps[atual[thread]]["sumarios"].append(bloco)
    return steps, job


def _consolidar(steps: dict) -> list[dict]:
    """Partições (`step:partitionN`) somadas no step manager; diverge = soma das partições ≠ sumário do manager."""
    grupos: dict[str, dict] = {}
    for nome, s in steps.items():
        base = m[1] if (m := PARTICAO.match(nome)) else nome
        g = grupos.setdefault(base, {"nome": base, "particoes": [], "manager": None})
        if m:
            g["particoes"].append(s)
        else:
            g["manager"] = s
    out = []
    for g in grupos.values():
        man = g["manager"] or {}
        if g["particoes"]:
            blocos = [b for p in g["particoes"] for b in p["sumarios"]]
            soma = {k: sum(b.get(k, 0) for b in blocos) for k in CONTADORES.values()}
            situacoes = {b.get("situacao") for b in blocos} - {None}
            soma["situacao"] = "COMPLETED" if situacoes == {"COMPLETED"} else ", ".join(sorted(situacoes)) or "?"
            agregado = man.get("sumarios", [None])[-1] if man.get("sumarios") else None
            diverge = agregado is not None and any(agregado.get(k) not in (None, soma[k]) for k in CONTADORES.values())
            dados = agregado if diverge else soma
            primeira = min(g["particoes"], key=lambda p: p.get("ts_ini", ""))
        else:
            dados, diverge, primeira = (man.get("sumarios") or [{}])[-1], False, man
        out.append({"nome": g["nome"], "dados": dados, "particoes": len(g["particoes"]), "diverge": diverge,
                    "inicio": man.get("inicio") or primeira.get("inicio"), "duracao": man.get("duracao"),
                    "ts_ini": man.get("ts_ini") or primeira.get("ts_ini", ""),
                    "ts_fim": man.get("ts_fim") or max((p.get("ts_fim", "") for p in g["particoes"]), default="")})
    return sorted(out, key=lambda s: s["ts_ini"])


def _n(v) -> str:
    return "?" if v is None else f"{v:,}".replace(",", ".")


def _assinatura(msg: str) -> str:
    """Primeira linha sem números/ids, para agrupar mensagens repetidas."""
    linha = (msg or "").strip().splitlines()[0] if (msg or "").strip() else ""
    return re.sub(r"\d+", "#", linha)[:120]


def _hora(ts: str) -> str:
    """Hora de Brasília (o ELK grava em UTC)."""
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(BRASILIA).strftime("%H:%M:%S")
    except ValueError:
        return ts


def relatorio(eventos: list[dict], erros: list[dict] | None, origem: str, aviso: str | None = None) -> str:
    """Markdown: cabeçalho do job, tabela por step (ordem de início) e WARN/ERROR agrupados (None = não varrido)."""
    steps, job = _steps(eventos)
    if not job.get("nome"):
        return f"Nenhuma execução de job encontrada ({origem})."
    lista = _consolidar(steps)
    status = job.get("status", "EM EXECUÇÃO ou sem linha de fim")
    icone = {"COMPLETED": "✅", "FAILED": "❌"}.get(status, "⚠️")
    duracao = job.get("duracao")
    if not duracao and (a := _dt(job.get("inicio", ""))) and (b := _dt(job.get("fim", ""))):
        duracao = str(b - a)
    out = [f"# Execução — {job['nome']} ({origem})", "",
           f"Início: {job.get('inicio', '?')} · Fim: {job.get('fim', '?')} · Status: {icone} {status} · "
           f"Duração: {duracao or '?'}"]
    if aviso:
        out += ["", f"> {aviso}"]
    out += ["", "## Steps", ""]
    for s in lista:
        d = s["dados"] or {}
        out += [f"| {s['nome']} | |", "|---|---|", f"| Situação | {d.get('situacao', '?')} |",
                f"| Linhas lidas / filtradas / gravadas | {_n(d.get('lidas'))} / {_n(d.get('filtradas'))} / "
                f"{_n(d.get('gravadas'))} |",
                f"| Skips (leitura / processamento / gravação) | {_n(d.get('skip_leitura'))} / "
                f"{_n(d.get('skip_processamento'))} / {_n(d.get('skip_gravacao'))} |",
                f"| Commits / rollback | {_n(d.get('commits'))} / {_n(d.get('rollback'))} |",
                f"| Início · duração | {s['inicio'] or '?'} · {s['duracao'] or '?'} |"]
        if s["particoes"]:
            out.append(f"| Partições | {s['particoes']} (consolidadas) |")
        if s["diverge"]:
            out.append("| ⚠️ Divergência | soma das partições ≠ sumário do manager; mostrado o do manager |")
        out.append("")
    alerta = [s["nome"] for s in lista if (s["dados"] or {}).get("rollback") or any(
        (s["dados"] or {}).get(k) for k in ("skip_leitura", "skip_processamento", "skip_gravacao"))]
    total = lambda k: _n(sum((s["dados"] or {}).get(k) or 0 for s in lista))  # noqa: E731
    out.append(f"{len(lista)} steps · lidas {total('lidas')} · gravadas {total('gravadas')}"
               + (f" · ⚠️ skip/rollback em: {', '.join(alerta)}" if alerta else ""))
    if erros is None:
        return "\n".join(out)
    out += ["", "## WARN / ERROR na janela", ""]
    if not erros:
        return "\n".join(out + ["Sem WARN/ERROR na janela da execução."])
    grupos: dict[tuple, list] = defaultdict(list)
    for e in erros:
        grupos[(e.get("level", "").upper(), e.get("logger", ""), e.get("excecao") or _assinatura(e["msg"]))].append(e)
    niveis = defaultdict(int)
    for (nivel, _, _), evs in grupos.items():
        niveis[nivel] += len(evs)
    out += [" · ".join(f"{n} {k}" for k, n in sorted(niveis.items())), "",
            "| Nível | Origem | Ocorrências | 1ª | Última | Mensagem (resumo) | Step |", "|---|---|---|---|---|---|---|"]
    for (nivel, logger, _), evs in sorted(grupos.items(), key=lambda kv: (kv[0][0] != "ERROR", -len(kv[1]))):
        evs.sort(key=lambda e: e["ts"])
        step = next((s["nome"] for s in lista if s["ts_ini"] and s["ts_ini"] <= evs[0]["ts"] <= (s["ts_fim"] or "~")),
                    "")
        resumo = ((evs[0]["msg"] or "").strip().splitlines() or [""])[0][:150].replace("|", "\\|")
        out.append(f"| {nivel} | {logger.rsplit('.', 1)[-1]} | {len(evs)}× | {_hora(evs[0]['ts'])} | "
                   f"{_hora(evs[-1]['ts'])} | {resumo} | {step} |")
    return "\n".join(out)
