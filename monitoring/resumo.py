"""Resumo do log de monitoramento (logs/pipeline.jsonl) pra o artifact.

    uv run python monitoring/resumo.py [--log logs] [--saida monitoring_out]

Lê o log inteiro (incluindo os arquivos rotacionados, pipeline.jsonl.N) e
escreve em --saida:

- resumo_execucao.md   -- a execução mais recente (ou --execucao ID) etapa
                          por etapa, chamadas ao LLM com tokens, resumo de
                          treino da semana e o histórico acumulado (latência
                          p50/p95 por etapa, desfechos, tokens, amostras).
- resumo_execucao.json -- o mesmo, em JSON.
- metrics.prom         -- formato de exposição do Prometheus (textfile).

Só biblioteca padrão: roda no runner do Actions e em qualquer máquina com
Python 3.12, sem instalar nada.
"""

import argparse
import json
import statistics
from collections import Counter, defaultdict
from pathlib import Path

BUCKETS_LATENCIA_MS = (100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000, 120000, 300000)


def ler_log(pasta: Path) -> list[dict]:
    """Todas as linhas, da mais antiga pra mais nova (pipeline.jsonl.5 ...
    pipeline.jsonl.1, pipeline.jsonl). Linha corrompida é pulada e contada."""
    arquivos = sorted(pasta.glob("pipeline.jsonl.*"), key=lambda p: -int(p.suffix[1:]) if p.suffix[1:].isdigit() else 0)
    arquivos.append(pasta / "pipeline.jsonl")
    linhas = []
    for arq in arquivos:
        if not arq.exists():
            continue
        for texto in arq.read_text(encoding="utf-8").splitlines():
            try:
                linhas.append(json.loads(texto))
            except json.JSONDecodeError:
                linhas.append({"evento": "_linha_invalida"})
    return linhas


def _percentil(valores: list[float], p: float) -> float | None:
    if not valores:
        return None
    if len(valores) == 1:
        return valores[0]
    return statistics.quantiles(valores, n=100, method="inclusive")[int(p) - 1]


def _ultimas_amostras(linhas: list[dict]) -> dict[str, dict[str, dict]]:
    """Por grupo, a última amostra de cada chave (os loaders reprocessam os
    mesmos dias em execuções seguidas)."""
    grupos: dict[str, dict[str, dict]] = defaultdict(dict)
    for l in linhas:
        if l.get("evento") == "amostra":
            grupos[l["grupo"]][l["chave"]] = l["valores"]
    return grupos


def _totais(etapas: list[dict], chamadas: list[dict]) -> dict:
    """Tempo e tokens da execução inteira. `duracao_total_ms` vai do início
    da primeira etapa ao fim da última (inclui instalação e espera entre os
    jobs do mesmo run); `soma_etapas_ms` é só o tempo dentro das etapas."""
    from datetime import datetime

    inicios = [datetime.fromisoformat(l["inicio"]) for l in etapas if l.get("inicio")]
    fins = [datetime.fromisoformat(l["ts"]) for l in etapas]
    ok = [c for c in chamadas if c.get("outcome") == "ok"]
    return {
        "duracao_total_ms": round((max(fins) - min(inicios)).total_seconds() * 1000) if inicios and fins else None,
        "soma_etapas_ms": sum(l["latency_ms"] for l in etapas),
        "etapas_com_erro": sum(1 for l in etapas if l["outcome"] == "erro"),
        "llm_chamadas": len(chamadas),
        "llm_chamadas_ok": len(ok),
        **{f"tokens_{t}": sum(c.get(f"tokens_{t}") or 0 for c in ok) for t in ("entrada", "saida", "raciocinio", "total")},
    }


def _plano(l: dict) -> dict:
    """O que a última análise (da execução, ou do log) produziu."""
    return {"ts": l["ts"], "outcome": l["outcome"], "latency_ms": l["latency_ms"], **l["saida"]}


def resumir(linhas: list[dict], execucao: str | None) -> dict:
    etapas = [l for l in linhas if l.get("evento") == "etapa"]
    chamadas = [l for l in linhas if l.get("evento") == "llm_chamada"]
    execucoes = list(dict.fromkeys(l["execucao_id"] for l in etapas))
    alvo = execucao or (execucoes[-1] if execucoes else None)

    latencias = defaultdict(list)
    desfechos = Counter()
    for l in etapas:
        latencias[l["etapa"]].append(l["latency_ms"])
        desfechos[(l["etapa"], l["outcome"])] += 1

    tokens = Counter()
    for c in chamadas:
        if c.get("outcome") == "ok":
            for tipo in ("entrada", "saida", "raciocinio", "total"):
                tokens[(c["modelo"], tipo)] += c.get(f"tokens_{tipo}") or 0

    amostras = _ultimas_amostras(linhas)
    semanas = sorted(amostras.get("semana", {}).items())
    sono = sorted(amostras.get("sono", {}).items())[-7:]

    def media(chave):
        vals = [v[chave] for _, v in sono if v.get(chave) is not None]
        return round(statistics.mean(vals), 2) if vals else None

    etapas_alvo = [l for l in etapas if l["execucao_id"] == alvo]
    chamadas_alvo = [c for c in chamadas if c["execucao_id"] == alvo]
    analises = [l for l in etapas_alvo if l["etapa"] == "analisar_com_ia"] or [
        l for l in etapas if l["etapa"] == "analisar_com_ia"
    ]
    return {
        "execucao_id": alvo,
        "execucao": {
            "etapas": etapas_alvo,
            "llm_chamadas": chamadas_alvo,
            "totais": _totais(etapas_alvo, chamadas_alvo),
        },
        "historico": {
            "linhas_no_log": len(linhas),
            "linhas_invalidas": sum(1 for l in linhas if l.get("evento") == "_linha_invalida"),
            "primeiro_registro": linhas[0].get("ts") if linhas else None,
            "ultimo_registro": linhas[-1].get("ts") if linhas else None,
            "execucoes": len(execucoes),
            "latencia_ms": {
                etapa: {
                    "n": len(v), "p50": _percentil(v, 50), "p95": _percentil(v, 95),
                    "min": min(v), "max": max(v),
                }
                for etapa, v in latencias.items()
            },
            "desfechos": [{"etapa": e, "outcome": o, "n": n} for (e, o), n in sorted(desfechos.items())],
            "llm_chamadas": len(chamadas),
            "llm_chamadas_por_outcome": dict(Counter(c["outcome"] for c in chamadas)),
            "tokens": [{"modelo": m, "tipo": t, "n": n} for (m, t), n in sorted(tokens.items())],
            "amostras_por_grupo": {g: len(v) for g, v in amostras.items()},
        },
        "amostras_no_log": bool(amostras),
        "plano": _plano(analises[-1]) if analises else None,
        "treino": {
            "semana_atual": dict(zip(("semana_inicio", "valores"), semanas[-1])) if semanas else None,
            "semana_anterior": dict(zip(("semana_inicio", "valores"), semanas[-2])) if len(semanas) > 1 else None,
            "sono_ultimas_noites": {
                "noites": len(sono),
                "sono_total_h_media": media("sono_total_h"),
                "sono_score_medio": media("sono_score"),
                "fc_repouso_media": media("fc_repouso"),
                "body_battery_ao_acordar_media": media("body_battery_ao_acordar"),
            },
        },
        "_latencias": latencias,
        "_desfechos": desfechos,
        "_tokens": tokens,
        "_chamadas": chamadas,
        "_amostras": amostras,
        "_etapas": etapas,
    }


def _seg(ms):
    return "-" if ms is None else f"{ms / 1000:.1f} s"


def _pace(seg):
    return f"{int(seg // 60)}:{int(seg % 60):02d}/km" if seg else "-"


def markdown(r: dict) -> str:
    h = r["historico"]
    out = [f"# Resumo do monitoramento -- execução `{r['execucao_id']}`", ""]

    tot = r["execucao"]["totais"]
    out += [
        "## Esta execução", "",
        f"- **Tempo total:** {_seg(tot['duracao_total_ms'])} do início da primeira etapa ao fim da última "
        f"({_seg(tot['soma_etapas_ms'])} dentro das etapas); {tot['etapas_com_erro']} etapa(s) com erro",
        f"- **Tokens consumidos:** {tot['tokens_total']} (entrada {tot['tokens_entrada']}, saída {tot['tokens_saida']}, "
        f"raciocínio {tot['tokens_raciocinio']}) em {tot['llm_chamadas_ok']} de {tot['llm_chamadas']} chamada(s) ao LLM",
        "", "| etapa | início | fim | latência (ms) | outcome | entrada | saída |", "|---|---|---|---:|---|---|---|"]
    for l in r["execucao"]["etapas"]:
        out.append(
            f"| {l['etapa']} | {l.get('inicio', '-')} | {l['ts']} | {l['latency_ms']} | {l['outcome']}{' (' + l['erro'] + ')' if l.get('erro') else ''} "
            f"| `{json.dumps(l['entrada'], ensure_ascii=False)}` | `{json.dumps(l['saida'], ensure_ascii=False)}` |"
        )
    if r["execucao"]["llm_chamadas"]:
        out += ["", "### Chamadas ao LLM", "", "| chamada | modelo | reserva | latência (ms) | outcome | tokens entrada | saída | raciocínio | total |", "|---|---|---|---:|---|---:|---:|---:|---:|"]
        for c in r["execucao"]["llm_chamadas"]:
            out.append(
                f"| {c['chamada']} | {c['modelo']} | {'sim' if c.get('modelo_reserva') else 'não'} | {c['latency_ms']} | {c['outcome']} "
                f"| {c.get('tokens_entrada', '-')} | {c.get('tokens_saida', '-')} | {c.get('tokens_raciocinio', '-')} | {c.get('tokens_total', '-')} |"
            )

    p = r["plano"]
    if p:
        out += [
            "", "## Plano gerado (última análise)", "",
            f"- {p['ts']}: outcome `{p['outcome']}`, {_seg(p['latency_ms'])}, modelo `{p.get('model_id', '-')}`, "
            f"{p.get('tentativas', '-')} tentativa(s), {p.get('revisoes_reprovadas', 0)} revisão(ões) reprovada(s)",
            f"- {p.get('dias_treino', '-')} dia(s) de treino, {p.get('dias_descanso', '-')} de descanso, "
            f"{p.get('dias_forca', 0)} com força; {p.get('minutos_planejados', '-')} min e {p.get('km_planejados', '-')} km planejados",
        ]

    t = r["treino"]
    out += ["", "## Treino (resumo, sem dado de dor)", ""]
    if not r["amostras_no_log"]:
        out.append(
            "- Amostras de domínio desligadas neste log (MONITORAMENTO_AMOSTRAS=0: dado real num repositório "
            "público). Com dado simulado (branch SR1), aqui aparecem semana (km, pace, ACWR) e sono."
        )
    for rotulo, chave in (("Semana atual", "semana_atual"), ("Semana anterior", "semana_anterior")):
        s = t[chave]
        if s:
            v = s["valores"]
            out.append(
                f"- **{rotulo}** ({s['semana_inicio']}): {v.get('sessoes')} sessões, {v.get('km_total')} km "
                f"({v.get('km_corridos')} km corridos), {v.get('tempo_correndo_min')} min correndo, "
                f"pace médio {_pace(v.get('pace_seg_km'))}, FC média {v.get('fc_media')}, ACWR {v.get('acwr')}"
            )
    so = t["sono_ultimas_noites"]
    if so["noites"]:
        out.append(
            f"- **Sono (últimas {so['noites']} noites):** {so['sono_total_h_media']} h em média, score {so['sono_score_medio']}, "
            f"FC de repouso {so['fc_repouso_media']}, body battery ao acordar {so['body_battery_ao_acordar_media']}"
        )

    out += [
        "", "## Histórico acumulado do log", "",
        f"- {h['linhas_no_log']} linhas ({h['linhas_invalidas']} inválidas), de {h['primeiro_registro']} a {h['ultimo_registro']}",
        f"- {h['execucoes']} execuções, {h['llm_chamadas']} chamadas ao LLM ({h['llm_chamadas_por_outcome']})",
        f"- Amostras por grupo (deduplicadas): {h['amostras_por_grupo']}",
        "", "| etapa | n | p50 (ms) | p95 (ms) | mín | máx |", "|---|---:|---:|---:|---:|---:|",
    ]
    for etapa, v in h["latencia_ms"].items():
        out.append(f"| {etapa} | {v['n']} | {round(v['p50'])} | {round(v['p95'])} | {v['min']} | {v['max']} |")
    out += ["", "| etapa | outcome | n |", "|---|---|---:|"]
    out += [f"| {d['etapa']} | {d['outcome']} | {d['n']} |" for d in h["desfechos"]]
    if h["tokens"]:
        out += ["", "| modelo | tokens | n |", "|---|---|---:|"]
        out += [f"| {d['modelo']} | {d['tipo']} | {d['n']} |" for d in h["tokens"]]
    return "\n".join(out) + "\n"


def _rotulos(**kv) -> str:
    return "{" + ",".join(f'{k}="{str(v).replace(chr(92), chr(92) * 2).replace(chr(34), chr(92) + chr(34))}"' for k, v in kv.items()) + "}"


def prometheus(r: dict) -> str:
    """Formato de exposição do Prometheus (pra node_exporter textfile
    collector ou pushgateway). Contadores acumulam o log inteiro."""
    out = []

    def metrica(nome, tipo, ajuda):
        out.extend([f"# HELP {nome} {ajuda}", f"# TYPE {nome} {tipo}"])

    metrica("garmin_pipeline_etapa_execucoes_total", "counter", "Execucoes de cada etapa por desfecho.")
    for (etapa, outcome), n in sorted(r["_desfechos"].items()):
        out.append(f"garmin_pipeline_etapa_execucoes_total{_rotulos(etapa=etapa, outcome=outcome)} {n}")

    metrica("garmin_pipeline_etapa_latencia_ms", "histogram", "Latencia de cada etapa em milissegundos.")
    for etapa, valores in sorted(r["_latencias"].items()):
        for b in BUCKETS_LATENCIA_MS:
            out.append(f"garmin_pipeline_etapa_latencia_ms_bucket{_rotulos(etapa=etapa, le=b)} {sum(1 for v in valores if v <= b)}")
        out.append(f"garmin_pipeline_etapa_latencia_ms_bucket{_rotulos(etapa=etapa, le='+Inf')} {len(valores)}")
        out.append(f"garmin_pipeline_etapa_latencia_ms_sum{_rotulos(etapa=etapa)} {sum(valores)}")
        out.append(f"garmin_pipeline_etapa_latencia_ms_count{_rotulos(etapa=etapa)} {len(valores)}")

    metrica("garmin_pipeline_etapa_ultima_execucao_timestamp", "gauge", "Carimbo ISO da ultima execucao de cada etapa (em label) e sua latencia.")
    ultima = {}
    for l in r["_etapas"]:
        ultima[l["etapa"]] = l
    for etapa, l in sorted(ultima.items()):
        out.append(f"garmin_pipeline_etapa_ultima_execucao_timestamp{_rotulos(etapa=etapa, outcome=l['outcome'], ts=l['ts'])} {l['latency_ms']}")

    metrica("garmin_pipeline_llm_chamadas_total", "counter", "Chamadas ao LLM por modelo e desfecho.")
    for (modelo, outcome), n in sorted(Counter((c["modelo"], c["outcome"]) for c in r["_chamadas"]).items()):
        out.append(f"garmin_pipeline_llm_chamadas_total{_rotulos(modelo=modelo, outcome=outcome)} {n}")

    metrica("garmin_pipeline_llm_tokens_total", "counter", "Tokens consumidos no LLM (usage_metadata) por modelo e tipo.")
    for (modelo, tipo), n in sorted(r["_tokens"].items()):
        out.append(f"garmin_pipeline_llm_tokens_total{_rotulos(modelo=modelo, tipo=tipo)} {n}")

    tot = r["execucao"]["totais"]
    metrica("garmin_pipeline_execucao", "gauge", "Totais da execucao mais recente (tempo em ms, tokens).")
    for chave, valor in tot.items():
        if valor is not None:
            out.append(f"garmin_pipeline_execucao{_rotulos(execucao_id=r['execucao_id'], variavel=chave)} {valor}")

    metrica("garmin_pipeline_amostras", "gauge", "Amostras de dominio no log, deduplicadas por chave.")
    for grupo, v in sorted(r["_amostras"].items()):
        out.append(f"garmin_pipeline_amostras{_rotulos(grupo=grupo)} {len(v)}")

    semana = r["treino"]["semana_atual"]
    if semana:
        metrica("garmin_treino_semana", "gauge", "Resumo da semana corrente (domingo a sabado).")
        for chave, valor in semana["valores"].items():
            if isinstance(valor, (int, float)) and not isinstance(valor, bool):
                out.append(f"garmin_treino_semana{_rotulos(semana_inicio=semana['semana_inicio'], variavel=chave)} {valor}")
    sono = r["treino"]["sono_ultimas_noites"]
    if sono["noites"]:
        metrica("garmin_sono_media_ultimas_noites", "gauge", "Media das ultimas noites registradas.")
        for chave, valor in sono.items():
            if chave != "noites" and valor is not None:
                out.append(f"garmin_sono_media_ultimas_noites{_rotulos(variavel=chave)} {valor}")
    return "\n".join(out) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--log", type=Path, default=Path("logs"), help="pasta com pipeline.jsonl (padrão: logs)")
    parser.add_argument("--saida", type=Path, default=Path("monitoring_out"), help="pasta de saída (padrão: monitoring_out)")
    parser.add_argument("--execucao", help="execucao_id a detalhar (padrão: a mais recente)")
    args = parser.parse_args()

    r = resumir(ler_log(args.log), args.execucao)
    args.saida.mkdir(parents=True, exist_ok=True)
    (args.saida / "resumo_execucao.md").write_text(markdown(r), encoding="utf-8")
    publico = {k: v for k, v in r.items() if not k.startswith("_")}
    (args.saida / "resumo_execucao.json").write_text(json.dumps(publico, ensure_ascii=False, indent=2), encoding="utf-8")
    (args.saida / "metrics.prom").write_text(prometheus(r), encoding="utf-8")
    print(f"[resumo de {r['historico']['linhas_no_log']} linha(s) do log escrito em {args.saida}/]")


if __name__ == "__main__":
    main()
