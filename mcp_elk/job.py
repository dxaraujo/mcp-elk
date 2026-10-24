"""Execução de job Spring Batch a partir das linhas dos listeners de job e de step ("...LoggerListener"):
sumário por step (partições consolidadas pela thread) e WARN/ERROR agrupados, em Markdown.

Evento = {ts, thread, logger, msg, level}; `ts` só ordena e recorta (no ELK é UTC); horários exibidos vêm da mensagem.
"""
from __future__ import annotations

import csv
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
SITUACAO = re.compile(r"Situa\S+o do step:\s*(\w+)")
TOTAL = re.compile(r"Total de ([^:]+?):\s*(\d+)")
PARTICAO = re.compile(r"^(.+?):partition(\d+)$")
CONTADORES = {"linhas lidas": "lidas", "skips na leitura": "skip_leitura", "linhas filtradas": "filtradas",
              "linhas gravadas": "gravadas", "skips na gravacao": "skip_gravacao",
              "skips no processamento": "skip_processamento", "commits": "commits", "rollback": "rollback"}
CSV_COLUNAS = ["ts", "thread", "logger", "level", "msg"]
# texto do Spring Boot: "2026-10-09T11:43:42.736-03:00  INFO 9266 --- [main] a.b.Classe : mensagem"
_LINHA_TEXTO = re.compile(r"^(\S+)\s+([A-Z]+)\s+\d+\s+---\s+\[\s*(.*?)\]\s+(\S+)\s*:\s?(.*)$")


def _sem_acento(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower().strip()


def chave_ordem(valor) -> tuple:
    """Chave de ordenação de um valor de tempo: data ISO com fração de qualquer tamanho (até ns) ou número."""
    texto = str(valor).strip()
    if texto.isdigit():
        return (int(texto), 0)
    m = re.match(r"^(.+?T\d\d:\d\d:\d\d)(?:[.,](\d+))?(.*)$", texto)
    if not m:
        raise ValueError(f"valor de `ordem` não é número nem data ISO: {texto}")
    dt = datetime.fromisoformat(m[1] + (m[3] or "").replace("Z", "+00:00"))
    return (int(dt.timestamp()), int((m[2] or "0").ljust(9, "0")[:9]))


def gravar_csv(eventos: list[dict], path: Path) -> None:
    """Eventos em CSV (UTF-8 com BOM), na ordem recebida; `ler_local` lê de volta."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, CSV_COLUNAS, extrasaction="ignore")
        w.writeheader()
        w.writerows(eventos)


def ler_local(caminho: str) -> list[dict]:
    """Eventos de um log local: CSV de `gravar_csv`, JSONL (preferido: message, logger_name, thread_name, level,
    @timestamp) ou texto do Spring Boot (linhas sem prefixo continuam a mensagem anterior)."""
    path = Path(caminho)
    if not path.is_absolute() or not path.is_file():
        raise ValueError(f"log_local precisa ser o caminho absoluto de um arquivo existente: {caminho}")
    if path.suffix.lower() == ".csv":
        with path.open(newline="", encoding="utf-8-sig") as f:
            return [{k: r.get(k) or "" for k in CSV_COLUNAS} for r in csv.DictReader(f)]
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
    """Da última "Iniciando processamento do JOB" (do `job`, ou de qualquer um) até o fim correspondente. Vale para ELK e
    log local: ordena antes pelo tempo com a fração inteira (até ns), que o ES e o arquivo podem não respeitar."""
    try:
        eventos = sorted(eventos, key=lambda e: chave_ordem(e["ts"]))
    except ValueError:  # tempo fora do ISO: fica a ordem recebida
        pass
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
    """{step: {inicio, fim, duracao, ts_ini, ts_fim, sumario}} + dados do job. No ELK as linhas do sumário são documentos
    soltos e as do mesmo milissegundo vêm em qualquer ordem: o sumário é montado por thread, sem depender do cabeçalho
    nem do "Finalizando step"; contador repetido abre o bloco seguinte, que vai para o step iniciado mais antigo da
    thread ainda sem sumário."""
    steps: dict[str, dict] = {}
    job: dict = {}
    fila: dict[str, list] = defaultdict(list)  # thread -> steps iniciados nela, em ordem
    aberto: dict[str, dict] = {}  # thread -> sumário em montagem

    def contar(thread: str, ts: str, chave: str, valor) -> None:
        if thread not in aberto or chave in aberto[thread]:
            livres = [n for n in fila[thread] if "sumario" not in steps[n]]
            if not livres:
                return
            # campo de tempo só em ms (ex.: o da ingestão): steps da mesma thread no mesmo ms podem trocar contadores
            nome = next((n for n in livres if steps[n]["ts_ini"] <= ts), livres[0])
            steps[nome]["sumario"] = aberto[thread] = {}
        aberto[thread][chave] = valor

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
                steps.setdefault(m[1], {}).update(inicio=m[2], ts_ini=e["ts"])
                fila[thread].append(m[1])
            elif m := FIM_STEP.search(linha):
                steps.setdefault(m[1], {}).update(fim=m[2], duracao=m[3], ts_fim=e["ts"])
            elif m := SITUACAO.search(linha):
                contar(thread, e["ts"], "situacao", m[1])
            elif (m := TOTAL.search(linha)) and (chave := CONTADORES.get(_sem_acento(m[1]))):
                contar(thread, e["ts"], chave, int(m[2]))
    return steps, job


def _consolidar(steps: dict) -> list[dict]:
    """Steps na ordem de início, cada um com suas partições (`X:partitionN`, em ordem de N). O dono das partições é o
    step de nome X ou, quando o worker tem outro nome, o último step iniciado antes delas (o manager)."""
    ordem = sorted(steps.items(), key=lambda kv: kv[1].get("ts_ini", ""))
    topo = {n: {"nome": n, **s, "particoes": []} for n, s in ordem if not PARTICAO.match(n)}
    grupos: dict[str, list] = defaultdict(list)
    for n, s in ordem:
        if m := PARTICAO.match(n):
            grupos[m[1]].append({"nome": f"partition{m[2]}", "n": int(m[2]), **s})
    for base, parts in grupos.items():
        parts.sort(key=lambda p: p["n"])
        primeira = min(p.get("ts_ini", "") for p in parts)
        dono = base if base in topo else next(
            (n for n, s in reversed(topo.items()) if s.get("ts_ini", "") <= primeira), None)
        if dono is None:  # manager sem linha de início
            dono = base
            topo[base] = {"nome": base, "ts_ini": primeira, "particoes": []}
        topo[dono]["particoes"] += parts
        topo[dono].setdefault("ts_fim", max((p.get("ts_fim", "") for p in parts), default=""))
    for s in topo.values():
        if s["particoes"]:
            blocos = [p.get("sumario") or {} for p in s["particoes"]]
            soma = {k: sum(b.get(k, 0) for b in blocos) for k in CONTADORES.values()}
            situacoes = {b.get("situacao") for b in blocos} - {None}
            soma["situacao"] = "COMPLETED" if situacoes == {"COMPLETED"} else ", ".join(sorted(situacoes)) or "?"
            s["soma"] = soma
            man = s.get("sumario") or {}
            s["diverge"] = any(man.get(k) not in (None, soma[k]) for k in CONTADORES.values())
    return sorted(topo.values(), key=lambda s: s.get("ts_ini", ""))


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
    out += ["", "## Steps", "", "| Step | Situação | Lidas | Filtradas | Gravadas | Skips | Commits | Rollback | Duração |",
            "|---|---|--:|--:|--:|--:|--:|--:|---|"]

    def linha(nome: str, d: dict, duracao: str | None) -> str:
        skips = [d[k] for k in ("skip_leitura", "skip_processamento", "skip_gravacao") if k in d]
        return (f"| {nome} | {d.get('situacao', '?')} | {_n(d.get('lidas'))} | {_n(d.get('filtradas'))} | "
                f"{_n(d.get('gravadas'))} | {_n(sum(skips) if skips else None)} | {_n(d.get('commits'))} | "
                f"{_n(d.get('rollback'))} | {duracao or '?'} |")

    for s in lista:
        out.append(linha(s["nome"], s.get("sumario") or {}, s.get("duracao")))
        for p in s["particoes"]:
            out.append(linha(f"↳ {p['nome']}", p.get("sumario") or {}, p.get("duracao")))
        if s["particoes"]:
            aviso_soma = " ⚠️ ≠ sumário do step" if s["diverge"] else ""
            out.append(linha(f"↳ **Σ {len(s['particoes'])} partições**{aviso_soma}", s["soma"], None))
    # sem total geral: somar lidas/gravadas de steps diferentes não tem sentido de negócio
    dados = [s.get("sumario") or s.get("soma") or {} for s in lista]  # o step carrega o total das suas partições
    alerta = [s["nome"] for s, d in zip(lista, dados) if d.get("rollback") or any(
        d.get(k) for k in ("skip_leitura", "skip_processamento", "skip_gravacao"))]
    if alerta:
        out += ["", f"⚠️ skip/rollback em: {', '.join(alerta)}"]
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
        step = next((s["nome"] for s in lista
                     if s.get("ts_ini") and s["ts_ini"] <= evs[0]["ts"] <= (s.get("ts_fim") or "~")), "")
        resumo = ((evs[0]["msg"] or "").strip().splitlines() or [""])[0][:150].replace("|", "\\|")
        out.append(f"| {nivel} | {logger.rsplit('.', 1)[-1]} | {len(evs)}× | {_hora(evs[0]['ts'])} | "
                   f"{_hora(evs[-1]['ts'])} | {resumo} | {step} |")
    return "\n".join(out)
