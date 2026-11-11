# mcp-elk

Servidor MCP para os logs do ELK (Elasticsearch + Kibana). Lê os logs; a única escrita é criar e atualizar busca
salva no Kibana. Desde a 3.0 é **autossuficiente**, sem power: o servidor lê a configuração do projeto
(`.sabia/config/elk/elk-<ambiente>.json`) e aplica sozinho índice, campo de tempo, recorte do sistema e apelidos de
campo; as regras de uso vão nas descrições das tools e nas `instructions` do MCP (veja [Como o modelo recebe as regras](#como-o-modelo-recebe-as-regras)), e o sumário de execução de
job Spring Batch é montado em Python, não pela LLM.

## Instalação

Publicado no [PyPI](https://pypi.org/project/mcp-elk/); requer o [uv](https://docs.astral.sh/uv/) (`uvx` no PATH).
No Kiro, em `~/.kiro/settings/mcp.json` (todos os projetos) ou `.kiro/settings/mcp.json` (um projeto):

```json
{
  "mcpServers": {
    "elk": {
      "type": "stdio",
      "command": "uvx",
      "args": ["mcp-elk@latest"],
      "env": {
        "ELK_USER": "${ELK_USER}",
        "ELK_PASSWORD": "${ELK_PASSWORD}",
        "ELK_URL_PROD": "https://logs.SEU-SERVIDOR",
        "UV_SYSTEM_CERTS": "true"
      }
    }
  }
}
```

**Como passar as variáveis:** defina-as no bloco `env` do servidor MCP, como no exemplo. Para não gravar segredos em
texto puro, use referências: `${VAR}` no Kiro IDE, `${env:VAR}` no Kiro CLI e `{env:VAR}` no opencode, e deixe o valor
no gerenciador de segredos ou no ambiente de onde a ferramenta é aberta. Não há arquivo de configuração nem caminho padrão.
No macOS, um app aberto pelo Dock não herda o `export` do shell: abra a ferramenta pelo terminal ou defina a variável no
ambiente da sessão. Evite `export` no perfil do shell quando o agente puder rodar comandos, porque ele conseguiria ler o valor.

`UV_SYSTEM_CERTS` faz o `uvx` usar as CAs da máquina para baixar o pacote (proxy corporativo).

Desenvolvimento:

```bash
uv sync
uv run pytest
uv run mcp dev mcp_elk/server.py   # MCP Inspector
```

## Como o modelo recebe as regras

O modelo não lê este README. As regras chegam por dois canais:

| Canal | Chega ao modelo? | O que vai nele |
|---|---|---|
| Descrição e parâmetros das tools | Sempre, em qualquer ferramenta | O essencial: começar por `elk_status`, passar `projeto`, contar antes, usar índices e apelidos da config |
| `instructions` do servidor | Só se a ferramenta repassar | O complemento: diagnóstico de incidente, job Spring Batch e passo a passo de configuração |

`projeto` é obrigatório no schema das tools; sem ele, a resposta traz um `aviso` de que a config do projeto não foi aplicada.

## Configuração

Só por variáveis de ambiente, no `env` do servidor MCP:

| Variável | Obrigatória | O que é |
|---|---|---|
| `ELK_USER`, `ELK_PASSWORD` | sim | Usuário e senha |
| `ELK_URL_PROD` | sim | URL do **Kibana** em produção |
| `ELK_URL_HOMOL`, `ELK_URL_DEV` | não | URL do **Kibana** nos outros ambientes |
| `ES_URL_PROD`, `ES_URL_HOMOL`, `ES_URL_DEV` | não | URL do **Elasticsearch** direto |
| `ELK_CA_BUNDLE` | não | Caminho de um arquivo PEM com a cadeia do Kibana, somada às CAs da máquina |

Com `ES_URL_<AMBIENTE>`, o MCP usa as APIs REST oficiais (`_search`, `_field_caps`, PIT). Sem elas, a busca passa pelo
Kibana em `POST /internal/search/es`, uma API **interna** do Kibana e o primeiro ponto a revalidar num upgrade.

A sessão aceita as cifras padrão do OpenSSL, porque
alguns Kibana só oferecem `AES256-SHA256`, que a lista do Python exclui.

## Configuração do projeto

`<projeto>/.sabia/config/elk/elk-<ambiente>.json` (`elk-prod.json`, `elk-homol.json`, `elk-dev.json`), validado pelo
[elk.schema.json](schemas/elk.schema.json) e gravado pela tool `elk_configurar` (peça ao agente "configurar o ELK").
Na leitura, o MCP procura nesta ordem: `.sabia/config/elk/`, `.kiro/config/elk/`, `.kiro/config/sdd/`; se o arquivo
existir em mais de uma, vale a primeira. A gravação vai sempre para `.sabia/config/elk/`.
Sem segredos:

```json
{
  "$schema": "https://raw.githubusercontent.com/dxaraujo/mcp-elk/main/schemas/elk.schema.json",
  "indice-padrao": "aplicacao",
  "indices": {
    "aplicacao": {
      "padrao": "logs-app-*",
      "campo-tempo": "@timestamp",
      "fixos": { "sistema.nome": "myapp*" },
      "campos": { "nivel": "log.level", "mensagem": "message", "servico": "service.name", "logger": "logger.name",
                  "thread": "process.thread.name", "excecao": "error.type", "stacktrace": "error.stack_trace" },
      "valores": { "nivel": ["DEBUG", "INFO", "WARN", "ERROR"] }
    }
  },
  "consultas": { "Erros críticos": { "id": "abc12345-...", "espaco": "producao" } },
  "download": { "path": "downloads/elk" }
}
```

- Toda tool recebe `projeto` (caminho **absoluto** do workspace: o cliente não inicia o MCP no workspace).
- `indice` = nome em `indices` (sem ele, `indice-padrao`) ou um padrão real (então sem fixos nem apelidos).
- `fixos` entram em toda consulta (recorte do sistema); valor com `*` vira `wildcard`.
- Apelidos de `campos` valem em `campos`, `consulta` (antes de `:`), `retornar` e `agrupar_por`.
- `salva` = título de `consultas` (ou id do Kibana): consulta, filtros e índice vêm da busca salva. Descrição com
  `Parâmetros: <campo>, ...` torna esses campos obrigatórios em `campos` e tira da consulta o filtro de exemplo.
- Sem o arquivo, as tools funcionam passando `indice` real (de `elk_listar_indices`).

## Tools

Parâmetros comuns: `projeto`, `ambiente` (prod/produção — padrão —, homol/homologação/hml, dev/desenvolvimento),
`inicio` (obrigatório; `now-15m` ou ISO 8601) e `fim`, `indice`, `campo_tempo`.

| Tool | Para quê |
|---|---|
| `elk_status` | Credencial, servidores e a config do projeto no ambiente (o que falta) |
| `elk_contar(..., agrupar_por?, top=10, comparar?)` | Dimensionar; os valores mais frequentes; `comparar='1d'` conta também a mesma janela antes e marca o que é `novo` (diagnóstico) |
| `elk_buscar_logs(..., limite=10≤100, retornar?)` | Poucos documentos na conversa: itens achatados, textos cortados em 15 linhas / 1.500 caracteres; `retornar` padrão = tempo + nivel, servico, logger, mensagem |
| `elk_exportar_csv(..., assunto? \| arquivo?, retornar?, max_linhas=100000)` | Volume grande: pagina e grava o CSV (em `download.path/<AAAAMMDD-HHmm>_<assunto>.csv`) sem passar documentos pela conversa |
| `elk_job(job?, inicio=now-7d, log_local?, erros=true)` | Última execução de job Spring Batch: baixa as linhas dos listeners de job e step em CSV no `download.path` (padrão `.kiro/local/elk`) e monta dele a tabela por step (o relatório também fica em `.md` ao lado) com uma linha por partição e a soma delas, conferida com o step e WARN/ERROR agrupados, em Markdown; do ELK (apelidos `mensagem`, `logger`, `thread`, `nivel`; `campo-tempo` de preferência o timestamp da aplicação, que ordena as linhas até o ns) ou de log local (de preferência o JSON, mesmo formato do Kibana; texto só sem JSON; ou o CSV baixado) |
| `elk_listar_consultas(busca?, espaco?)` | Buscas salvas e saved queries do Kibana (todos os spaces, ou só `espaco`) |
| `elk_obter_consulta(salva)` | Detalhes de uma consulta salva |
| `elk_criar_consulta(titulo, ...)` | Cria busca salva do Discover (fixos + `campos` viram pílulas; `consulta` na barra, kuery ou lucene) e registra o título na config |
| `elk_atualizar_consulta(id, ...)` | Troca só o informado (`campos`/`filtros` substituem todos os filtros) e preserva ordenação, período e layout |
| `elk_listar_indices(busca?, espaco?)` | Data views (padrão de índice e campo de tempo) |
| `elk_listar_campos(busca?, indice?)` | Campos, tipo e se são agregáveis (até `limite=100`) |
| `elk_configurar(projeto, config)` | Valida e grava `elk-<ambiente>.json` |

Resposta: um único TextContent, sem `structuredContent` — `{"ok":true,"dados":{...}}` ou
`{"ok":false,"erro":{"codigo","detalhe","campo"}}` (códigos: `config`, `credencial`, `certificado`, `indisponivel`,
`nao_encontrado`, `permissao`, `entrada`); `elk_job` devolve Markdown.

### Grandes volumes

O ELK tem milhões de documentos por hora; o MCP e as `instructions` seguem esta ordem:

1. **Conte antes** (`elk_contar`), com janela curta.
2. **Na conversa, só as primeiras linhas:** `elk_buscar_logs` traz 10 documentos por padrão (até 100), só os campos de
   `retornar`, e corta stack traces e mensagens no começo.
3. **Resultado grande:** `elk_exportar_csv` grava o CSV em disco; o agente lê só o cabeçalho e as primeiras linhas
   (`head -n 5`) e filtra, conta ou agrupa com um script Python (módulo `csv`) que imprime só o resultado — nunca o
   CSV inteiro na conversa.

### Consulta

- `consulta` é **Lucene** (`query_string`): `campo:valor AND msg:*timeout*`. O KQL do Kibana é convertido no navegador
  e o ES 8.15 não o entende. Por isso o MCP passa `and`/`or`/`not` para maiúsculas fora de aspas: sem isso, `a: x and b: y`
  virava OR e trazia centenas de vezes mais documentos. Comparações KQL (`campo > 0`, `campo >= 10`) viram `campo:>0`.
  `campo:{...}` aninhado continua sem suporte.
- `campos` = filtros exatos `{campo: valor}` (`term`); valor com `*` vira `wildcard`. `filtros` = Query DSL crua.
- Exportação: com ES direto, usa PIT + `search_after` (consistente). Só com o Kibana, usa `search_after` por
  `campo_tempo` e descarta os `_id` repetidos na virada da página; a página cresce para acomodar os repetidos e só dá
  erro (pedindo `es_<ambiente>`) acima de 10.000 documentos no mesmo timestamp.

### Job Spring Batch

Premissa: o projeto loga com listeners de job e de step (classes com `LoggerListener` no nome) nas mensagens
`Iniciando/Finalizando processamento do JOB: <job> na data <dd/MM/yyyy HH:mm:ss>[, com status <STATUS>]`,
`Iniciando/Finalizando step: <step> ... [com duração de HH:MM:SS]`, `##### SUMÁRIO DE EXECUCAO #####`,
`Situação do step: <STATUS>` e `Total de <linhas lidas|linhas filtradas|linhas gravadas|skips na leitura|skips no
processamento|skips na gravação|commits|rollback>: <n>`. Os horários exibidos são os da mensagem; o campo de tempo do
ELK (UTC) só recorta a janela (início e fim do job ± 5 s).
