"""Log de monitoramento do pipeline: um arquivo texto append-only, uma linha
JSON por evento (JSON Lines), em `logs/pipeline.jsonl`.

O banco e a planilha guardam o estado do domínio (a vida do corredor); este
arquivo guarda a operação -- o que entrou e saiu de cada etapa, quanto
demorou, como terminou. No GitHub Actions a máquina some no fim do job, então
o workflow carrega o arquivo de uma execução pra outra (cache) e o publica
como artifact (ver monitoring/README.md).

Três eventos (`evento`):

- `etapa`: uma linha por etapa do pipeline (`load_atividades`, ...,
  `analisar_com_ia`), com `latency_ms`, `outcome` e os dicionários
  `entrada` e `saida` (contagens de lote, sinais de guardrail, tamanho do
  prompt, tokens...).
- `llm_chamada`: uma linha por chamada ao Gemini -- modelo, latência,
  tokens de entrada/saída/raciocínio, desfecho (inclusive a troca pro modelo
  de reserva).
- `amostra`: uma observação de uma variável de domínio (`grupo` = sono,
  sessao, semana ou dia_planejado), a matéria-prima de histogramas,
  boxplots, KS e PSI. Os loaders reprocessam os últimos dias a cada sync,
  então a mesma data pode aparecer em várias execuções: a análise deduplica
  por (`grupo`, `chave`) ficando com a mais recente.

Privacidade (o repositório é público, e o artifact também): nenhuma linha
leva segredo, nome, e-mail, id de atividade, texto livre ou registro de dor;
erro vira só o nome da exceção (a mensagem pode trazer URL do banco ou
token). As amostras de domínio (sono, FC, pace) são agregados numéricos sem
identificação, e podem ser desligadas com MONITORAMENTO_AMOSTRAS=0 -- o log
de operação continua completo.

Data e hora: ISO 8601 com milissegundos e fuso de Brasília
(2026-11-16T03:17:05.123-03:00), na chave `ts`.
"""

import json
import logging
import os
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

from src.config import ROOT
from src.tempo import FUSO_BRT

ARQUIVO_PADRAO = ROOT / "logs" / "pipeline.jsonl"

# Rotação: o arquivo cresce ~poucos KB por execução; 5 MB x 5 backups dá
# anos de histórico. Rotacionar não quebra o append-only: a linha antiga vai
# pra pipeline.jsonl.1 intacta.
TAMANHO_MAXIMO = 5 * 1024 * 1024
BACKUPS = 5

VERSAO_FORMATO = 1


def _execucao() -> dict:
    """Identifica a execução: no Actions, o run (e a tentativa, pra ver os
    retries) -- os jobs do mesmo run compartilham o id; local, um id por
    processo, ou MONITORAMENTO_EXECUCAO_ID pra agrupar vários processos
    (ex.: as 5 etapas do sync rodadas à mão)."""
    if os.environ.get("MONITORAMENTO_EXECUCAO_ID"):
        return {"execucao_id": os.environ["MONITORAMENTO_EXECUCAO_ID"], "workflow": "local", "origem": "local"}
    if os.environ.get("GITHUB_RUN_ID"):
        return {
            "execucao_id": f"{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}",
            "workflow": os.environ.get("GITHUB_WORKFLOW", ""),
            "origem": "github_actions",
        }
    return {"execucao_id": f"local-{uuid.uuid4().hex[:8]}", "workflow": "local", "origem": "local"}


class _FormatoJson(logging.Formatter):
    """Uma linha JSON por registro. `formatTime` com fuso explícito em vez do
    `datefmt` + time.localtime padrão: o runner do Actions roda em UTC, e o
    carimbo tem que bater com o calendário do corredor (BRT)."""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:  # noqa: N802 -- nome da API do logging
        return datetime.fromtimestamp(record.created, FUSO_BRT).isoformat(timespec="milliseconds")

    def format(self, record: logging.LogRecord) -> str:
        linha = {
            "ts": self.formatTime(record),
            "nivel": record.levelname,
            "v": VERSAO_FORMATO,
            **_EXECUCAO,
            "evento": record.getMessage(),
            **getattr(record, "dados", {}),
        }
        return json.dumps(linha, ensure_ascii=False, default=str)


_EXECUCAO = _execucao()
_logger: logging.Logger | None = None


def _log() -> logging.Logger:
    global _logger
    if _logger is None:
        arquivo = Path(os.environ.get("MONITORAMENTO_ARQUIVO") or ARQUIVO_PADRAO)
        arquivo.parent.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(arquivo, mode="a", maxBytes=TAMANHO_MAXIMO, backupCount=BACKUPS, encoding="utf-8")
        handler.setFormatter(_FormatoJson())
        _logger = logging.getLogger("garmin.monitoramento")
        _logger.setLevel(logging.INFO)
        _logger.addHandler(handler)
        # Só no arquivo: o console do Actions é público (ver config.EM_CI).
        _logger.propagate = False
    return _logger


def registrar(evento: str, nivel: int = logging.INFO, **dados) -> None:
    """Escreve uma linha no log. Monitoramento nunca derruba o pipeline: uma
    falha aqui (disco, serialização) é engolida."""
    try:
        _log().log(nivel, evento, extra={"dados": dados})
    except Exception:  # noqa: BLE001 -- best-effort de propósito
        pass


def _amostras_ligadas() -> bool:
    return os.environ.get("MONITORAMENTO_AMOSTRAS", "1") != "0"


def amostra(grupo: str, chave: str, **valores) -> None:
    """Uma observação de domínio (ver docstring do módulo). `chave` identifica
    a observação dentro do grupo (a data, o início da semana...)."""
    if _amostras_ligadas():
        registrar("amostra", grupo=grupo, chave=chave, valores=valores)


# --------------------------------------------------------------------------
# Etapas
# --------------------------------------------------------------------------

class Etapa:
    """O que uma etapa registra enquanto roda: `entrada` e `saida` são
    dicionários livres; `outcome` começa vazio (vira "ok" no fim, "erro" com
    exceção) e a etapa pode fixar outro (pulado_idempotente, suspenso...)."""

    def __init__(self, nome: str):
        self.nome = nome
        self.entrada: dict = {}
        self.saida: dict = {}
        self.outcome: str | None = None

    def contar(self, chave: str, n: int = 1) -> None:
        self.saida[chave] = self.saida.get(chave, 0) + n


_etapa_atual: Etapa | None = None


def etapa_atual() -> Etapa | None:
    """A etapa em andamento -- pra código mais fundo (Gemini, Garmin)
    anotar sem precisar receber o objeto por parâmetro."""
    return _etapa_atual


def contar(chave: str, n: int = 1) -> None:
    if _etapa_atual is not None:
        _etapa_atual.contar(chave, n)


@contextmanager
def etapa(nome: str):
    """Mede e registra uma etapa:

        with etapa("load_atividades") as e:
            e.entrada["atividades_api"] = len(novas)
            ...
            e.saida["carregadas"] = n

    `SystemExit(0)` (saída antecipada de propósito, como plano já escrito)
    não é erro: fica o outcome que a etapa fixou, ou "ok"."""
    global _etapa_atual
    e = Etapa(nome)
    anterior, _etapa_atual = _etapa_atual, e
    inicio = time.perf_counter()
    erro = None
    try:
        yield e
    except SystemExit as saida:
        if saida.code not in (None, 0):
            e.outcome, erro = "erro", "SystemExit"
        raise
    except BaseException as exc:
        e.outcome, erro = "erro", type(exc).__name__
        raise
    finally:
        _etapa_atual = anterior
        registrar(
            "etapa",
            logging.ERROR if erro else logging.INFO,
            etapa=nome,
            outcome=e.outcome or "ok",
            latency_ms=round((time.perf_counter() - inicio) * 1000),
            erro=erro,
            entrada=e.entrada,
            saida=e.saida,
        )


# --------------------------------------------------------------------------
# Amostras de domínio (consultas prontas, pra instrumentação ficar curta)
# --------------------------------------------------------------------------

def _num(v, casas: int = 2):
    return None if v is None else round(float(v), casas)


def _horas(seg) -> float | None:
    return None if seg is None else round(seg / 3600, 2)


def amostras_sono(dias: list[dict]) -> int:
    """Uma amostra por noite (dict de extract.garmin.recuperacao_diaria).
    Noite sem dado de sono (relógio fora do pulso) não vira amostra, mas é
    contada -- qualidade do dado também é métrica."""
    sem_sono = 0
    for d in dias:
        if d.get("sono_total_seg") is None:
            sem_sono += 1
            continue
        amostra(
            "sono", str(d["data"]),
            sono_total_h=_horas(d.get("sono_total_seg")),
            sono_profundo_h=_horas(d.get("sono_profundo_seg")),
            sono_rem_h=_horas(d.get("sono_rem_seg")),
            sono_desperto_h=_horas(d.get("sono_desperto_seg")),
            despertares=d.get("despertares"),
            sono_score=d.get("sono_score"),
            sono_fc_media=_num(d.get("sono_fc_media"), 1),
            fc_repouso=d.get("fc_repouso"),
            body_battery_ao_acordar=d.get("body_battery_ao_acordar"),
            estresse_medio_dia=_num(d.get("estresse_medio_dia"), 1),
        )
    return sem_sono


def amostras_sessoes(con, activity_ids: list) -> None:
    """Uma amostra por sessão carregada, já com a classificação e o tempo
    correndo calculados no banco (vw_sessoes). Sem activity_id no log."""
    if not activity_ids:
        return
    linhas = con.execute(
        """
        SELECT data, hora_inicio, tipo, tipo_treino, distancia_km, km_corridos, duracao_min,
               segundos_correndo, pace_min_km, fc_media, fc_maxima, cadencia_media,
               efeito_treino_aerobico,
               (COALESCE(tempo_zona_fc_4, 0) + COALESCE(tempo_zona_fc_5, 0))
                 / NULLIF(COALESCE(tempo_zona_fc_1, 0) + COALESCE(tempo_zona_fc_2, 0) + COALESCE(tempo_zona_fc_3, 0)
                          + COALESCE(tempo_zona_fc_4, 0) + COALESCE(tempo_zona_fc_5, 0), 0)
        FROM vw_sessoes WHERE activity_id = ANY(%s) ORDER BY data
        """,
        (list(activity_ids),),
    ).fetchall()
    for (data, hora, tipo, tipo_treino, km, km_corridos, duracao, seg_correndo, pace,
         fc_media, fc_maxima, cadencia, efeito, frac_z45) in linhas:
        # chave = início da sessão (duas no mesmo dia não colidem na deduplicação)
        amostra(
            "sessao", hora.isoformat(timespec="minutes") if hora is not None else str(data),
            tipo_garmin=tipo,
            tipo_treino=tipo_treino,
            distancia_km=_num(km),
            km_corridos=_num(km_corridos),
            duracao_min=_num(duracao, 1),
            tempo_correndo_min=_num(seg_correndo / 60 if seg_correndo is not None else None, 1),
            fracao_correndo=_num(seg_correndo / 60 / float(duracao) if seg_correndo is not None and duracao else None, 3),
            pace_seg_km=_num(float(pace) * 60 if pace is not None else None, 0),
            fc_media=fc_media,
            fc_maxima=fc_maxima,
            cadencia_media=_num(cadencia, 0),
            efeito_aerobico=_num(efeito, 1),
            fracao_z4_z5=_num(frac_z45, 3),
        )


def amostras_semana(con, semanas: int = 2) -> None:
    """Resumo das últimas `semanas` semanas (domingo a sábado): volume, pace
    médio, tempo correndo e ACWR. A semana corrente aparece parcial em cada
    sync diário; a última amostra de cada `chave` é a semana fechada."""
    linhas = con.execute(
        """
        WITH s AS (
            SELECT data - EXTRACT(DOW FROM data)::int AS semana_inicio,
                   COUNT(*) AS sessoes,
                   COUNT(*) FILTER (WHERE tipo_treino <> 'Caminhada' AND tipo_treino <> 'Força') AS sessoes_corrida,
                   SUM(distancia_km) AS km_total,
                   SUM(km_corridos) AS km_corridos,
                   SUM(duracao_min) AS duracao_min,
                   SUM(segundos_correndo) / 60.0 AS tempo_correndo_min,
                   SUM(duracao_min) FILTER (WHERE distancia_km > 0) / NULLIF(SUM(distancia_km), 0) AS pace_min_km,
                   AVG(fc_media) AS fc_media
            FROM vw_sessoes WHERE tipo_treino IS DISTINCT FROM 'Força'
            GROUP BY 1
        )
        SELECT s.*, r.acwr, r.carga_cronica_km
        FROM s LEFT JOIN vw_resumo_semanal r USING (semana_inicio)
        ORDER BY semana_inicio DESC LIMIT %s
        """,
        (semanas,),
    ).fetchall()
    for (inicio, sessoes, sessoes_corrida, km_total, km_corridos, duracao, correndo,
         pace, fc_media, acwr, cronica) in linhas:
        amostra(
            "semana", str(inicio),
            sessoes=sessoes,
            sessoes_corrida=sessoes_corrida,
            km_total=_num(km_total),
            km_corridos=_num(km_corridos),
            duracao_min=_num(duracao, 1),
            tempo_correndo_min=_num(correndo, 1),
            pace_seg_km=_num(float(pace) * 60 if pace is not None else None, 0),
            fc_media=_num(fc_media, 1),
            acwr=_num(acwr),
            carga_cronica_km=_num(cronica),
        )
