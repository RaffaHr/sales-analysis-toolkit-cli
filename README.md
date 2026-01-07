# Sales Insight Toolkit

Ferramenta interativa em Python para explorar vendas históricas, devoluções e estratégia de portfólio a partir da base `BASE.xlsx`. O projeto organiza a lógica em módulos independentes e gera relatórios em arquivos Excel separados por análise.

---

## 1. Visão Geral

- **Entrada**: planilha Excel com aba `VENDA` e as colunas listadas adiante.
- **Saída**: arquivos `.xlsx` gerados na pasta `output/`, cada um com as guias relevantes da análise escolhida.
- **Uso**: menu no terminal que permite selecionar tipo de análise, recorte de período, filtros por categoria e parâmetros adicionais (ranking, janela recente, etc.).

---

## 2. Estrutura do Projeto

```
main.py                 # ponto de entrada da aplicação
analysis/
    __init__.py
    cli.py              # fluxo interativo/menu
    data_loader.py      # leitura e padronização da planilha
    exporters.py        # geração dos relatórios Excel
    reporting/
        __init__.py
        returns.py      # análise de devoluções críticas
        potential.py    # análise de SKUs com potencial de retomada
        top_history.py  # ranking histórico de SKUs recorrentes
        low_cost.py     # busca de produtos baratos para reputação
      product_focus.py # análise de performance de venda
```

---

## 3. Pré-requisitos

1. Python 3.10 ou superior.
2. Dependências instaladas (sugestão: criar um ambiente virtual):

   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install pandas openpyxl numpy
   ```

3. Arquivo `BASE.xlsx` na raiz do projeto com a aba `VENDA` (ou divisões `VENDA01`, `VENDA02`, ...) e colunas abaixo.

---

## 4. Colunas Esperadas na Aba `VENDA`

| Coluna original (Excel)            | Coluna normalizada (DataFrame) | Descrição resumida                                      |
|-----------------------------------|--------------------------------|---------------------------------------------------------|
| DATA                              | `data`                         | Data completa do pedido no formato `dd/mm/aaaa`.       |
| NR_NOTA_FISCAL                    | `nr_nota_fiscal`               | Identificador único da nota/pedido (uma linha por pedido).|
| CATEGORIA                         | `categoria`                    | Segmento ou linha de produto.                          |
| CD_PRODUTO                        | `cd_produto`                   | Código interno do SKU.                                 |
| DS_PRODUTO                        | `ds_produto`                   | Descrição comercial do SKU.                            |
| CD_FABRICANTE                     | `cd_fabricante`                | Código do SKU no fabricante/parceiro.                  |
| TP_ANUNCIO                        | `tp_anuncio`                   | Tipo de anúncio (ex.: produto final, kit, variação).   |
| Qtd de pedido                     | `qtd_pedidos`                  | Indicador legado de pedidos (mantido para referência). |
| Qtd de sku no pedido              | `qtd_sku`                      | Quantidade total de unidades vendidas.                 |
| ROB                               | `rob`                          | Receita Bruta observada (quando disponível).           |
| Preco vendido                     | `preco_vendido`                | Preço unitário praticado no pedido.                    |
| Perc Margem Bruta% RBLD           | `perc_margem_bruta`            | Margem bruta percentual (0 a 1).                       |
| Custo do produto                  | `custo_produto`                | Custo unitário de aquisição/estoque.                   |
| Qtd Produto Devolvido             | `qtd_devolvido`                | Quantidade devolvida no período.                       |
| Devolução Receita Bruta Tot$      | `devolucao_receita_bruta`      | Valor bruto das devoluções.                            |

> **Importante:** o carregamento converte nomes para minúsculo, remove espaços extras e normaliza a coluna `DATA` para `datetime`. As colunas derivadas `ano_mes` e `periodo` continuam disponíveis para compatibilidade, sendo calculadas automaticamente a partir da data. Campos de texto como `cd_fabricante` e `tp_anuncio` são preenchidos com padrões quando ausentes. Demais colunas são preservadas, embora não utilizadas nas análises atuais.

---

## 5. Enriquecimento Automático de Dados


1. **Datas padronizadas**: a coluna `data` é convertida para `datetime` (interpretando `dd/mm/aaaa`) e normalizada para meia-noite. A partir dela são gerados `periodo` (`Period[M]`) e `ano_mes` (`YYYYMM`) para manter comparações mensais.
2. **Coerção numérica**: remove símbolos (`%`, vírgula decimal) e converte a `float`. Percentuais acima de 1 são ajustados para escala 0-1 (ex.: `25` vira `0.25`).
3. **Normalização de texto**: `categoria`, `cd_produto`, `ds_produto`, `cd_fabricante` e `tp_anuncio` são preenchidos com valores padrão e aparados.
4. **Métricas derivadas**:
   - `receita_bruta_calc = preco_vendido * qtd_sku`
   - `rob = ROB` quando informado; caso contrário usa `receita_bruta_calc`
   - `custo_total = custo_produto * qtd_sku`
   - `lucro_bruto_estimado = receita_bruta_calc * perc_margem_bruta`
   - `taxa_devolucao = qtd_devolvido / qtd_sku` (com proteção contra divisão por zero)
   - `pedidos` (em todas as análises) = contagem de notas fiscais distintas (`nr_nota_fiscal`)

Essas colunas servem de base para as análises subsequentes.

> **Formato dos percentuais**: todos os campos de taxa ou margem nos relatórios finais são exibidos como texto com duas casas decimais e o símbolo `%` (ex.: `7,00%`).
> **Referência de preços**: as análises comparativas adicionam `preco_min_intervalo` (menor preço no intervalo filtrado) e `preco_min_historico_total` (menor preço observado em todo o histórico) para cada SKU.

> **Bases fracionadas em várias abas**: quando a planilha superar o limite de linhas do Excel, use abas sequenciais como `VENDA01`, `VENDA02`, etc. O carregador identifica automaticamente todas as abas que começam com `VENDA`, combina os dados e mantém as mesmas etapas de limpeza.
> **Cache automático**: após a primeira execução, o dataset pré-processado é salvo em `.cache/`. Se o `BASE.xlsx` não mudar, próximas execuções reutilizam esse cache e pulam a leitura pesada do Excel.
> **Progresso em tempo real**: o CLI exibe o percentual real de linhas já carregadas (0–100%) antes de iniciar os cálculos das análises, oferecendo feedback imediato em bases volumosas.

---
## 6. Executando o Script

```powershell
python main.py
```

Passo a passo:
1. Informe o número da análise desejada.
2. Visualize os períodos disponíveis e informe o intervalo de datas no formato `DD/MM/AAAA` (ou pressione Enter para analisar todo o histórico, inclusive com comparações diárias).
3. Escolha a categoria (ou "Todas" para analisar o portfólio completo). Na análise de performance de venda você pode optar entre filtrar por categoria ou informar uma lista de `CD_PRODUTO`.
4. Para análises que pedem ranking, defina o tamanho desejado (10, 20, 50, 100 ou outro valor positivo).
5. Na análise de potencial, personalize a janela recente se desejar (quantidade de meses e períodos específicos).
6. Aguarde a geração e anote o caminho do arquivo exibido no console.
7. Escolha "s" para continuar analisando ou "n" para encerrar.

---

## 7. Descrição das Análises

### 7.1 Análise de Devolução (`RETURN`)

**Objetivo**: encontrar produtos com alto volume vendido e taxa de devolução igual ou superior a 20% em um mês específico.

**Como funciona**:
- Dados agrupados por mês (`periodo`) e SKU.
- Métricas por mês: `qtd_vendida`, `pedidos`, `qtd_devolvida`, `receita`, `receita_devolucao`.
- `taxa_devolucao = qtd_devolvida / qtd_vendida` (apresentada em `%` com duas casas decimais).
- Filtro principal: `taxa_devolucao >= 0.20` **e** `qtd_vendida >= 40` unidades no mês.

**Relatórios gerados**:
- `resumo_produto`: consolida meses ativos, totais de venda/devolução/receita, pedidos (notas únicas) e `taxa_devolucao_total`.
- `picos_por_mes`: lista os períodos críticos com a taxa de devolução mensal e os volumes envolvidos.
- `visao_mensal`: resume cada mês com `produtos_afetados`, `total_devolvido`, `total_vendido` e `pedidos_totais` relevantes.


---

### 7.2 Análise de SKU em Potencial de Venda (`POTENTIAL`)

**Objetivo**: destacar SKUs que apresentavam histórico forte e caíram recentemente, mas ainda têm potencial de retomada.

**Processo**:
1. Agrupa vendas por mês e SKU, calculando médias de `qtd_vendida`, `pedidos`, `receita`, `custo`, `margem`, `qtd_devolvida`.
2. Divide o histórico em duas janelas:
   - **Histórica**: todos os meses exceto os `RECENT_WINDOW` finais (padrão 3). Se houver poucos meses, a janela é ajustada automaticamente.
   - **Recente**: últimos `RECENT_WINDOW` meses.
3. Para cada janela calcula:
   - `qtd_vendida_media_*`, `receita_media_*`, `pedidos_medios_*`, `taxa_devolucao_media_*`, `margem_media_*`, `*meses_validos`.
4. Métricas comparativas:
   - `queda_abs_qtd = qtd_vendida_media_historico - qtd_vendida_media_recente`
   - `queda_pct_qtd = queda_abs_qtd / qtd_vendida_media_historico`
   - `potencial_score = queda_abs_qtd_positiva * historico_meses_validos * (1 - taxa_devolucao_media_historico)`
5. Filtros mínimos:
   - `historico_meses_validos >= 3`
   - `qtd_vendida_media_historico` acima da mediana dos elegíveis
   - `queda_pct_qtd >= 0.30`
   - `taxa_devolucao_media_recente <= 0.20`

**Relatórios gerados**:
- `potenciais`: top N (rank escolhido) com quedas percentuais, taxas de devolução, margens em `%`, além dos preços mínimos (`preco_min_intervalo`, `preco_min_historico_total`).
- `hist_mensal`: histórico apenas dos SKUs selecionados, trazendo o preço mínimo por período e referências históricas para entender a trajetória de cada SKU.

**Intuito**: evidenciar oportunidades para campanhas de reativação, ajustes de estoque ou revisão de posicionamento comercial.

> 💡 Durante a execução é possível definir manualmente a janela recente: informe a quantidade de meses a analisar e selecione exatamente quais períodos (ano/mês) serão comparados com o histórico.

---

### 7.3 Análise de Top SKUs Mais Vendidos Historicamente (`TOP_SELLERS`)

**Objetivo**: ranquear SKUs com melhor consistência ao longo do tempo.

**Passos**:
1. Agrupa por mês e SKU: `qtd_vendida`, `pedidos`, `receita`, `devolucao`, `margem`.
2. Consolida por SKU:
   - `meses_com_venda`, `quantidade_total`, `pedidos_total`, `receita_total`, `devolucao_total`, `margem_media`.
   - `taxa_devolucao_total = devolucao_total / quantidade_total`.
3. Filtra SKUs com pelo menos 3 meses com venda (`MIN_MONTHS_RECURRENCE`).
4. Ordena por `meses_com_venda`, `quantidade_total`, `receita_total` (todos decrescentes) e seleciona o ranking.
5. Calcula `ticket_medio_estimado = receita_total / pedidos_total`.

**Relatórios gerados**:
- `ranking`: tabela final com pedidos totais (notas distintas), taxa de devolução, margem média em `%`, ticket médio, preço médio do intervalo e referências de preço (`preco_min_intervalo`, `preco_min_historico_total`).
- `detalhe_mensal`: visão mensal dos SKUs ranqueados, incluindo margens em `%`, preço médio do mês e preços mínimos para acompanhar a competitividade.

**Intuito**: apoiar decisões de sortimento principal, planejamento de estoque e reconhecimento de best sellers sustentáveis.

---

### 7.4 Análise de Produto de Custo Baixo para Reputação (`REPUTATION`)

**Objetivo**: encontrar produtos baratos que vendem bem e devolvem pouco, ideais para reforço de reputação, campanhas de entrada ou aumento de conversão.

**Metodologia**:
1. Agrupa por SKU, calculando:
   - `quantidade_total`, `pedidos_total`, `receita_total`, `custo_medio_unitario`, `custo_total`, `devolucao_total`, `receita_devolucao_total`, `margem_media`.
   - `taxa_devolucao = devolucao_total / quantidade_total`.
   - `ticket_medio_estimado = receita_total / pedidos_total`.
2. Determina `custo_threshold` = percentil 25 de `custo_medio_unitario` (padrão `cost_percentile = 0.25`).
3. Filtros aplicados:
   - `custo_medio_unitario <= custo_threshold`
   - `quantidade_total >= 50`
   - `taxa_devolucao <= 0.05`
4. Calcula a pontuação:
   - `potencial_reputacao_score = ((1 - taxa_devolucao) * quantidade_total) / custo_medio_unitario`
   - Para custos zero ou negativos, o denominador vira 1, evitando distorções.

**Relatório gerado**:
- `produtos_indicados`: tabela com todas as métricas acima (taxas e margens em `%`), acrescida dos preços mínimos (`preco_min_intervalo`, `preco_min_historico_total`) e ordenada por `potencial_reputacao_score` decrescente.

**Intuito**: priorizar itens que ajudam na percepção de valor da loja ao oferecer produtos baratos, com giro e baixo risco de devolução.

---

### 7.5 Análise de performance de venda (`PRODUCT_FOCUS`)

**Objetivo**: diagnosticar rapidamente o desempenho comercial dentro de um intervalo de datas, seja por categoria ou por uma lista específica de SKUs.

**Como funciona**:
- Após definir o período, escolha na CLI se deseja filtrar por uma categoria ou informar os `CD_PRODUTO` de interesse.
- Quando a escolha for por categoria, todo o portfólio filtrado é avaliado; quando optar por `CD_PRODUTO`, somente os SKUs informados são considerados (mesmo que pertençam a categorias distintas).
- A análise gera três visões complementares:
   - `resumo_produtos`: consolida o desempenho total no intervalo (receita, pedidos, margem média, ticket médio, devoluções, custo total, lucro estimado) junto com `cd_fabricante` e `tp_anuncio`.
   - `analise_diaria`: mostra a evolução dia a dia, com métricas de pedidos, quantidade vendida, ticket médio, preços praticados e taxas de devolução.
   - `analise_mensal`: agrega os mesmos indicadores por mês (`periodo`), útil quando o recorte cobre mais de um mês.

**Intuito**: comparar rapidamente campanhas, reposições, lançamentos ou uma categoria inteira para decidir se a performance está dentro do esperado.

---

## 8. Personalização

- Parâmetros como limiares de devolução, janelas de análise e tamanhos mínimos podem ser ajustados diretamente nos arquivos de `analysis/reporting/`.
- Para novas análises, basta criar um módulo similar e registrá-lo em `analysis/cli.py`.

---

## 9. Boas Práticas e Próximos Passos

- Revisar periodicamente se a base Excel está aderente às colunas esperadas.
- Validar os relatórios gerados com as equipes comercial e de operações.
- Incorporar testes automatizados à medida que novas regras de negócio forem adicionadas.