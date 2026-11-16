# Monitoramento: log de execução do pipeline

Cada volta do pipeline (o sync diário e o plano semanal) deixa um recibo num
arquivo texto **append-only**, `logs/pipeline.jsonl`: o que entrou em cada
etapa, o que saiu, quanto demorou, como terminou. No GitHub Actions a máquina
some no fim do job; o Neon, a planilha, o e-mail e o relógio guardam a vida do
corredor, e o log de operação fica num **artifact** que dá pra baixar depois.

- Quem escreve: [`src/monitoramento.py`](../src/monitoramento.py) (módulo
  `logging` da biblioteca padrão, `RotatingFileHandler`).
- Quem resume: [`monitoring/resumo.py`](resumo.py) (só biblioteca padrão).
- Onde é chamado: o `__main__` de cada etapa (`src/load/*`,
  `src/analyze/analisar_com_ia.py`) e as chamadas ao Gemini
  (`src/analyze/gemini.py`).

## 1. Disparar e baixar (GitHub Actions)

Pré-requisito: [`gh`](https://cli.github.com/) autenticado, na raiz do clone.

```bash
# sync diário (5 etapas: atividades, recuperação, dor, alertas, planilha)
gh workflow run "Sync atividades"

# plano semanal (chama o sync antes e depois a análise com o Gemini);
# forcar=true gera um plano novo mesmo se a semana já tem um
gh workflow run "Análise semanal" -f semana=proxima -f forcar=false

# acompanhar e baixar
gh run list --workflow "Sync atividades" --limit 5
gh run watch                       # escolhe a execução em andamento
gh run download <run-id>           # baixa todos os artifacts do run
gh run download <run-id> -n monitoramento-sync-<run-id>-1   # só um
```

Na branch SR1 (sem relógio), acrescente `--ref SR1` aos `gh workflow run`.

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
| `ts` | `2026-11-16T03:17:05.123-03:00` | ISO 8601, milissegundos, fuso de Brasília (o runner roda em UTC) |
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

`outcome`: `ok`, `erro` (com `erro` = nome da exceção, nunca a mensagem),
`pulado_idempotente` (plano da semana já existia), `suspenso_alerta`,
`suspendeu_treinos`, `alerta_sem_pendencia`. Um retry do workflow aparece
como uma linha `erro` seguida de outra da mesma etapa no mesmo `execucao_id`.

### `llm_chamada` -- uma linha por chamada ao Gemini

```json
{"evento": "llm_chamada", "chamada": "geração do plano", "modelo": "gemini-3.8-flash",
 "modelo_reserva": false, "outcome": "ok", "latency_ms": 23110,
 "prompt_chars": 41234, "prompt_tokens_contados": 12873,
 "tokens_entrada": 12873, "tokens_saida": 1650, "tokens_raciocinio": 2210,
 "tokens_total": 16733, "finish_reason": "STOP", "resposta_chars": 5120}
```

Sobrecarga (`erro_503`, `erro_429`) gera uma linha e a chamada seguinte com
`modelo_reserva: true` -- a troca de modelo fica visível.

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
| `semana` | semana dom-sáb (sessões, km, tempo correndo, pace médio, FC média, ACWR) | planilha_desempenho |
| `dia_planejado` | dia do plano gerado (tipo, km, min, força) | analisar_com_ia |

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
| `acwr`, `teto_sessao_km`, `restricao_dor`, `alerta_recente`, `regime`, `retomada_trote_bloco_seg`, `instrucoes_guardrail` | entrada da IA | 3.1 restrição de vocabulário por guardrail determinístico; 1.4 "sinalização de risco de sobrecarga" |
| `tentativas`, `violacoes_regras`, `revisoes_reprovadas` | saída da IA | 3.7 o erro mais caro é progressão de intensidade indevida -- quantas vezes o LLM tentou e o Python barrou |
| `modelo`, `modelo_reserva`, tokens, `prompt_chars` | entrada/saída da IA | 3.8-8 troca/descontinuação do modelo (aconteceu: 3.5 → 3.8); custo e teto de tokens (Fase 10) |
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
identificação. Na versão com dado real (branch `main`, repositório público),
dá pra desligá-las sem mexer no código: variável do repositório
`MONITORAMENTO_AMOSTRAS=0` (Settings → Secrets and variables → Actions →
Variables, ou `gh variable set MONITORAMENTO_AMOSTRAS --body 0`). O log de
operação continua completo. Na SR1 o dado é simulado.

## 5. Gerar carga

Cada execução acrescenta ao mesmo log (cache), então carga = várias
execuções:

```bash
# 5 syncs seguidos (cada um ~2-4 min; o concurrency do workflow enfileira)
for i in 1 2 3 4 5; do gh workflow run "Sync atividades"; sleep 20; done

# 2-3 planos (forcar=true pra não sair como pulado_idempotente e chamar o Gemini)
gh workflow run "Análise semanal" -f semana=proxima -f forcar=true
```

**Na SR1 (sem relógio):** a Garmin simulada já entrega ~10 semanas de
histórico no primeiro sync -- mas os loaders só pedem os últimos 7 dias, então
o primeiro sync rende ~7 noites e as sessões da semana; as amostras crescem
uma noite por dia de sync. Pra ter distribuição logo, rode o sync várias vezes
(acima) e a análise com `forcar=true` 2-3 vezes: o corredor simulado segue o
plano, então as sessões novas também mudam.

**Local** (com `.env` preenchido), agrupando as etapas numa execução só:

```bash
export MONITORAMENTO_EXECUCAO_ID="local-$(date +%s)"
for m in load_atividades load_recuperacao load_dor alertas planilha_desempenho; do
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
