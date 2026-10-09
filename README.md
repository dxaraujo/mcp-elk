# mcp-elk

Servidor MCP para o ELK (Elasticsearch + Kibana): busca, contagem com agrupamento, consultas prontas do Kibana e
exportação CSV, só de leitura nos logs. As únicas escritas são criar e atualizar busca salva no Kibana. É **agnóstico**: índice, campos (ex.: sistema, categoria) e consultas do projeto chegam como
parâmetro.

## Credenciais e servidores

| SO | Arquivo |
|---|---|
| Linux/Mac | `~/.config/mcp-elk/elk.properties` |
| Windows | `%APPDATA%\mcp-elk\elk.properties` |

Para usar outro caminho, defina a variável de ambiente `MCP_ELK_CONFIG`.

```ini
[DEFAULT]
user = SEU_USUARIO
password = SUA_SENHA
prod = https://logs.SEU-SERVIDOR
homol = https://hlogs.SEU-SERVIDOR
dev = https://dlogs.SEU-SERVIDOR
```

`prod`, `homol` e `dev` são as URLs do **Kibana**; só `prod` é obrigatória. Opcionalmente, `es_prod`, `es_homol` e
`es_dev` são as URLs do **Elasticsearch** direto: quando presentes, o MCP usa as APIs REST oficiais (`_search`,
`_field_caps`, PIT). Sem elas, a busca passa pelo Kibana em `POST /internal/search/es`, uma API **interna** do Kibana e
o primeiro ponto a revalidar num upgrade.

**Certificados:** o MCP confia nas CAs instaladas na máquina (no Windows, as do repositório do Windows) e, se existir,
em `elk-bundle.pem`, na mesma pasta do `elk.properties`: a cadeia PEM do Kibana, quando a CA
não estiver instalada. `REQUESTS_CA_BUNDLE` também funciona. Se o certificado não for aceito, a tool devolve uma mensagem
dizendo onde gravar a cadeia. A sessão aceita as cifras padrão do OpenSSL, porque alguns Kibana só oferecem
`AES256-SHA256`, que a lista do Python exclui.

## Instalação

Publicado no [PyPI](https://pypi.org/project/mcp-elk/); requer o [uv](https://docs.astral.sh/uv/) (`uvx` no PATH).
No cliente MCP (`mcp.json`):

```json
{
  "mcpServers": {
    "elk": {
      "type": "stdio",
      "command": "uvx",
      "args": ["mcp-elk@latest"],
      "env": { "UV_SYSTEM_CERTS": "true" }
    }
  }
}
```

`UV_SYSTEM_CERTS` faz o `uvx` usar as CAs da máquina para baixar o pacote (proxy corporativo).

Desenvolvimento:

```bash
uv sync
uv run pytest
uv run mcp dev mcp_elk/server.py   # MCP Inspector
```

## Tools

Parâmetros comuns:
- `ambiente`: prod/produção (padrão), homol/homologação/hml ou dev/desenvolvimento;
- `inicio`/`fim`: `now-15m` ou ISO 8601. `inicio` é obrigatório;
- `campo_tempo`: padrão `@timestamp`.

| Tool | Para quê |
|---|---|
| `elk_buscar_logs(indice, inicio, consulta?, campos?, filtros?, limite≤100, retornar?)` | Investigar: itens achatados (`{"a.b": v}`), textos cortados em 2000 caracteres |
| `elk_contar(..., agrupar_por?, top=10)` | Dimensionar; os valores mais frequentes de um campo |
| `elk_exportar_csv(..., arquivo, retornar?, max_linhas=100000)` | Volume grande: pagina e grava o CSV em disco, sem passar documentos pela conversa |
| `elk_listar_consultas(busca?, espaco?, limite=50)` | Buscas salvas do Discover e saved queries do Kibana (todos os spaces, ou só `espaco`): `id`, `espaco`, `tipo`, `titulo` |
| `elk_obter_consulta(id, espaco?)` | Detalhes de uma consulta salva: `consulta`, `filtros` (DSL) e `indice` prontos para reuso |
| `elk_criar_consulta(titulo, indice, consulta?, campos?, filtros?, colunas?, descricao?, espaco=default, linguagem=kuery)` | Cria uma busca salva do Discover ligada ao data view de `indice` no `espaco`: `campos`/`filtros` viram filtros (pílulas), `consulta` vai na barra de busca (kuery ou lucene) |
| `elk_atualizar_consulta(id, espaco=default, titulo?, indice?, consulta?, campos?, filtros?, colunas?, descricao?, linguagem?)` | Atualiza uma busca salva: troca só o informado (`campos`/`filtros` substituem todos os filtros) e preserva ordenação, período e layout do Kibana |
| `elk_listar_indices(busca?, espaco?)` | Data views (padrão de índice e campo de tempo) |
| `elk_listar_campos(indice, busca?)` | Campos, tipo e se são agregáveis |

- `consulta` é **Lucene** (`query_string`): `campo:valor AND msg:*timeout*`. O KQL do Kibana é convertido no navegador
  e o ES 8.15 não o entende. Por isso o MCP passa `and`/`or`/`not` para maiúsculas fora de aspas: sem isso, `a: x and b: y`
  virava OR e trazia centenas de vezes mais documentos. Comparações KQL (`campo > 0`, `campo >= 10`) viram `campo:>0`.
  `campo:{...}` aninhado continua sem suporte.
- `campos` = filtros exatos `{campo: valor}` (`term`); valor com `*` vira `wildcard` (`{"sistema.nome": "app*"}` pega
  `app`, `app-worker`, `app_batch`). `filtros` = Query DSL crua.
- Exportação: com ES direto, usa PIT + `search_after` (consistente). Só com o Kibana, usa `search_after` por
  `campo_tempo` e descarta os `_id` repetidos na virada da página. Com documentos demais no mesmo timestamp, dá
  erro e pede `es_<ambiente>`. A página cresce para acomodar os documentos repetidos, e o erro só aparece quando passa de
  10.000 documentos no mesmo timestamp.
