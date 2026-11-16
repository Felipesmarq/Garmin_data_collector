# Monitoramento: log de execução do pipeline

> **Branch SR1** (Garmin simulada, sem relógio): mesmo log da `main`, com as
> diferenças desta branch -- 4 etapas no sync (sem `alertas`), plano sem
> envio ao relógio, semana de segunda a domingo e guardrails só de ACWR e dor.

Cada volta do pipeline (o sync diário e o plano semanal) deixa um recibo num
arquivo texto **append-only**, `logs/pipeline.jsonl`: o que entrou em cada
etapa, o que saiu, quanto demorou, como terminou. No GitHub Actions a máquina
some no fim do job; o Neon, a planilha, o e-mail e o relógio guardam a vida do
corredor, e o log de operação fica num **artifact** que dá pra baixar depois.

- Quem escreve: [`src/monitoramento.py`](../src/monitoramento.py) (módulo
  `logging` da biblioteca padrão, `RotatingFileHandler`).
- Quem resume: [`monitoring/resumo.py`](resumo.py) (só biblioteca padrão).
- Onde é chamado: o `__main__` de cada etapa (`src/load/*`,
  `src/analyze/analisar_com_ia.py`) e as chamadas ao Gemini (`_gerar_json`
  em `src/analyze/analisar_com_ia.py`).

## 1. Disparar e baixar (GitHub Actions)

Pré-requisito: [`gh`](https://cli.github.com/) autenticado, na raiz do clone.

```bash
# sync diário (4 etapas: atividades, recuperação, dor, planilha)
gh workflow run "Sync atividades"

# plano semanal (chama o sync antes e depois a análise com o Gemini);
# forcar=true gera um plano novo mesmo se a semana já tem um
gh workflow run "Análise semanal" -f semana=proxima -f forcar=false

# ENSAIO: Gemini de verdade, tudo no log, mas sem planilha nem e-mail
# (outcome=ensaio) -- o jeito seguro de gerar carga da análise
gh workflow run "Análise semanal" -f semana=proxima -f entregar=false

# acompanhar e baixar
gh run list --workflow "Sync atividades" --limit 5
gh run watch                       # escolhe a execução em andamento
gh run download <run-id>           # baixa todos os artifacts do run
gh run download <run-id> -n monitoramento-sync-<run-id>-1   # só um
```

Num fork com a SR1 como branch padrão (ver README da SR1), os comandos acima
já rodam nela; no repositório original, acrescente `--ref SR1`.

### Nome do artifact

| Workflow | Artifact |
|---|---|
| Sync atividades | `monitoramento-sync-<run_id>-<tentativa>` |
| Análise semanal | `monitoramento-sync-<run_id>-<tentativa>` (o job de sync que ela chama) e `monitoramento-analise-<run_id>-<tentativa>` |

Retenção: 30 dias.

### O que abrir

```
logs/pipeline.jsonl          o log (append-only); pipeline.jsonl.1..5 se rotacionou
monitoring_out/
  resumo_execucao.md         comece por aqui: a execução etapa por etapa,
                             chamadas ao LLM com tokens, resumo de treino
                             e sono, histórico acumulado (p50/p95, desfechos)
  resumo_execucao.json       o mesmo, em JSON
  metrics.prom               formato de exposição do Prometheus
```

### Por que o artifact traz o histórico, não só a execução

Uma execução sozinha tem poucas linhas (5 etapas no sync, 1 na análise) -- não
dá distribuição nenhuma. O workflow carrega o arquivo de uma execução pra
outra com `actions/cache` (restaura o mais recente no começo do job, salva com
chave nova no fim, `if: always()`), então o arquivo continua só crescendo e
cada artifact leva o log acumulado inteiro. Isso também mantém o append-only
de verdade: nada é reescrito, só acrescentado.

## 2. Formato do log

JSON Lines: uma linha = um JSON completo. Lê-se com um `cat`, e vira tabela
com `pandas.read_json("logs/pipeline.jsonl", lines=True)`.

Campos comuns a toda linha:

| campo | exemplo | |
|---|---|---|
| `ts` | `2026-11-16T03:17:05.123-03:00` | ISO 8601, milissegundos, fuso de Brasília (o runner roda em UTC); numa `etapa`, é o fim -- o início vem em `inicio` |
| `nivel` | `INFO` / `ERROR` | nível do `logging` |
| `v` | `1` | versão do formato |
| `execucao_id` | `19384756-1` | run do Actions + tentativa (jobs do mesmo run compartilham) |
| `workflow` / `origem` | `Sync atividades` / `github_actions` | |
| `evento` | `etapa` / `llm_chamada` / `amostra` | tipo da linha (abaixo) |

### `etapa` -- uma linha por etapa

```json
{"ts": "...", "evento": "etapa", "etapa": "load_recuperacao", "outcome": "ok",
 "latency_ms": 8421, "erro": null,
 "entrada": {"dias_pedidos": 7, "dias_com_dado": 7},
 "saida": {"dias_gravados": 7, "noites_sem_sono": 1}}
```

`outcome`: `ok`, `erro` (com `erro` = nome da exceção, nunca a mensagem;
`SIGTERM` quando o timeout do workflow mata a tentativa), `pulado_idempotente`
(plano da semana já existia), `ensaio` (plano gerado e não entregue). Um retry
do workflow aparece como uma linha `erro` seguida de outra da mesma etapa no
mesmo `execucao_id`.

### `llm_chamada` -- uma linha por chamada ao Gemini

```json
{"evento": "llm_chamada", "chamada": "geração do plano", "modelo": "gemini-3.8-flash",
 "modelo_reserva": false, "outcome": "ok", "latency_ms": 23110,
 "prompt_chars": 41234, "prompt_tokens_contados": 12873,
 "tokens_entrada": 12873, "tokens_saida": 1650, "tokens_raciocinio": 2210,
 "tokens_total": 16733, "finish_reason": "STOP", "resposta_chars": 5120}
```

Sobrecarga (`erro_503`, `erro_429`) gera uma linha com o código; na SR1 não
há modelo de reserva (`modelo_reserva` é sempre `false`), então a falha vai
pro retry do workflow -- e aparece como nova `llm_chamada` na tentativa
seguinte.

### `amostra` -- uma observação de domínio

```json
{"evento": "amostra", "grupo": "sono", "chave": "2026-11-15",
 "valores": {"sono_total_h": 6.4, "sono_profundo_h": 1.1, "sono_score": 74,
             "fc_repouso": 52, "body_battery_ao_acordar": 71, ...}}
```

| grupo | uma amostra por | escrita por |
|---|---|---|
| `sono` | noite | load_recuperacao |
| `sessao` | atividade (km, km corridos, tempo correndo, pace, FC, cadência, fração em Z4-Z5, tipo de treino) | load_atividades |
| `semana` | semana seg-dom (sessões, km, tempo correndo, pace médio, FC média, ACWR) | planilha_desempenho |
| `dia_planejado` | dia do plano gerado (tipo, km, min) | analisar_com_ia |

Os loaders reprocessam os últimos 7 dias a cada sync, então a mesma `chave`
aparece em execuções seguidas: **deduplique por (`grupo`, `chave`) ficando
com a última** (é o que o `resumo.py` faz). A semana corrente aparece parcial
em cada sync; a última amostra da chave é a semana fechada.

## 3. Por que essas variáveis (CRISP-DM)

A escolha segue o [Entendimento do Negócio](../docs/ENTENDIMENTO_DO_NEGOCIO.md):

| Variável no log | Entrada/saída | Liga com |
|---|---|---|
| `outcome`, `latency_ms`, retries por etapa | operação | 1.4 "operação contínua do pipeline sem intervenção manual"; 1.4 "entrega semanal automática" |
| `outcome=pulado_idempotente` | operação | idempotência ponta a ponta (cron + disparo manual não gera dois planos) |
| contagens de lote (atividades, dias, linhas, splits, `noites_sem_sono`) | entrada | 3.6 entradas antecipadas; qualidade do dado (relógio fora do pulso) |
| `acwr`, `guardrail_acwr`, `restricao_dor`, `instrucoes_guardrail` | entrada da IA | 3.1 restrição de vocabulário por guardrail determinístico; 1.4 "sinalização de risco de sobrecarga" |
| `tentativas`, `revisoes_reprovadas` | saída da IA | 3.7 o erro mais caro é progressão de intensidade indevida -- quantas vezes a revisão barrou o plano |
| `modelo`, tokens, `prompt_chars` | entrada/saída da IA | 3.8-8 troca/descontinuação do modelo (aconteceu: 3.5 → 3.8); custo e teto de tokens (Fase 10) |
| `dia_planejado` (km, min, tipo) | saída da IA | 3.5 variável-alvo (plano semanal, unidade = dia); 3.8-7 validação quantitativa da saída |
| `status_*` (dentro/fora da margem, não realizado) | saída | 3.8-1 comparar o plano com o realizado |
| `sessao` (km corridos, tempo correndo, pace, FC, Z4-Z5) | entrada | 3.6 histórico de atividades; 1.3 evolução sem repetir o padrão de carga da lesão |
| `sono` (total, profundo, score, FC de repouso, body battery) | entrada | 3.6 "tendência de sono, FC de repouso e nível de energia"; recuperação ruim como sinal de sobrecarga |
| `semana` (km, pace médio, ACWR) | saída agregada | 1.3 objetivo de negócio; resumo pedido no artifact |

Pra montar depois (próxima parte): histogramas e boxplots saem direto das
amostras e das `latency_ms`/tokens; KS e PSI comparam uma janela de
referência (ex.: as 8 semanas mais antigas) com a atual (as últimas 4, a
mesma janela do prompt), por variável. Sono é o grupo com mais pontos (uma
noite por dia), sessões o mais ligado ao risco de lesão.

## 4. O que **não** vai pro log

- Segredos, tokens, URLs, e-mail, nome, `activity_id`.
- Nível de dor, localização da dor, comentários, observações e qualquer texto
  livre (inclusive o plano e os motivos -- só números e categorias).
- Mensagem de exceção (pode conter URL do banco): só o nome da classe.
- Nada do log vai pro console do Actions (o log do job é público).

As amostras de domínio (sono, FC, pace) são agregados numéricos sem
identificação. Na SR1 o dado é simulado; na versão com dado real (branch `main`,
repositório público), dá pra desligá-las sem mexer no código: variável do repositório
`MONITORAMENTO_AMOSTRAS=0` (Settings → Secrets and variables → Actions →
Variables, ou `gh variable set MONITORAMENTO_AMOSTRAS --body 0`). O log de
operação continua completo.

## 5. Gerar carga

Cada execução acrescenta ao mesmo log (cache), então carga = várias
execuções:

```bash
# 5 syncs seguidos (cada um ~2-4 min; o concurrency do workflow enfileira)
for i in 1 2 3 4 5; do gh workflow run "Sync atividades"; sleep 20; done

# vários planos em ensaio (chamam o Gemini, não entregam nada)
for i in 1 2 3; do gh workflow run "Análise semanal" -f entregar=false; sleep 20; done
```

Cada ensaio gera 2+ linhas `llm_chamada` (geração e revisão, mais as
tentativas que o Python reprovar) e 7 amostras `dia_planejado`. Ensaios
seguidos dão a distribuição de tokens, latência do LLM e km/minutos
planejados -- e mostram a variabilidade do LLM com o mesmo contexto.

**Na SR1 (sem relógio):** os loaders pedem a janela inteira da Garmin
simulada a cada sync, então **o primeiro sync já rende ~10 semanas de
amostras** (uma noite por dia, ~3 sessões por semana) -- distribuição pronta
pra histogramas, KS e PSI desde a primeira execução. Os syncs seguintes
repetem as mesmas chaves (deduplicadas) e acrescentam operação (latência,
desfechos). Rode a análise em ensaio algumas vezes pra ter a distribuição de
tokens e de dias planejados.

**Local** (com `.env` preenchido), agrupando as etapas numa execução só:

```bash
export MONITORAMENTO_EXECUCAO_ID="local-$(date +%s)"
for m in load_atividades load_recuperacao load_dor planilha_desempenho; do
  uv run python -m src.load.$m
done
uv run python monitoring/resumo.py && cat monitoring_out/resumo_execucao.md
```

## 6. Prometheus (opcional)

`metrics.prom` está no formato de exposição texto do Prometheus: dá pra servir
com o *textfile collector* do node_exporter ou empurrar pra um Pushgateway.
Métricas:

| métrica | tipo | rótulos |
|---|---|---|
| `garmin_pipeline_etapa_execucoes_total` | counter | etapa, outcome |
| `garmin_pipeline_etapa_latencia_ms` | histogram | etapa |
| `garmin_pipeline_etapa_ultima_execucao_timestamp` | gauge (latência da última) | etapa, outcome, ts |
| `garmin_pipeline_llm_chamadas_total` | counter | modelo, outcome |
| `garmin_pipeline_llm_tokens_total` | counter | modelo, tipo |
| `garmin_pipeline_amostras` | gauge | grupo |
| `garmin_treino_semana` | gauge | semana_inicio, variavel |
| `garmin_sono_media_ultimas_noites` | gauge | variavel |
