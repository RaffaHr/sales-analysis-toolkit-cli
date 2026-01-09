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
   pip install pandas openpyxl numpy xlsxwriter
   ```

3. Arquivo `BASE.xlsx` na raiz do projeto com a aba `VENDA` (ou divisões `VENDA01`, `VENDA02`, ...) e colunas abaixo.

---

## 4. Colunas Esperadas nas Abas de Entrada

### 4.1 Aba de Vendas (`VENDA`, `VENDA01`, ...)

| Coluna original (Excel)             | Coluna normalizada | Descrição resumida                                       |
|------------------------------------|--------------------|----------------------------------------------------------|
| DATA_VENDA                         | `data`             | Data da venda (`dd/mm/aaaa`), usada para construir os períodos. |
| NOTA_FISCAL_VENDA                  | `nr_nota_fiscal`   | Identificador único do pedido/nota fiscal.              |
| CATEGORIA                          | `categoria`        | Segmento ou linha de produto.                           |
| CD_ANUNCIO                         | `cd_anuncio`       | Código único do anúncio ofertado (identificador comercial). |
| DS_ANUNCIO                         | `ds_anuncio`       | Descrição comercial do anúncio publicada.               |
| CD_PRODUTO                         | `cd_produto`       | Código interno do SKU.                                  |
| DS_PRODUTO                         | `ds_produto`       | Descrição comercial do SKU.                             |
| CD_FABRICANTE                      | `cd_fabricante`    | Código do fabricante ou parceiro.                       |
| TP_ANUNCIO                         | `tp_anuncio`       | Tipo de anúncio (produto, kit, variação, etc.).         |
| Unidades                           | `qtd_sku`          | Quantidade de unidades vendidas na linha.               |
| Preco Medio Unit$ / Preço Medio Unit$ | `preco_vendido` | Preço unitário praticado na nota.                       |
| Custo Medio$ / Custo Médio$        | `custo_produto`    | Custo médio unitário informado na planilha.             |
| Perc Margem Bruta% RBLD            | `perc_margem_bruta`| Margem bruta percentual (0 a 1).                         |
| Receita Bruta (-) Devoluções Tot$  | `rbld`             | Receita líquida de devoluções, quando disponível.       |
| TP_REGISTRO                        | `tp_registro`      | Controle interno da planilha para diferenciar o tipo de linha. |

### 4.2 Abas de Devolução (`DEVOLUCAO`, `DEVOLUCAO01`, ...)

| Coluna original (Excel)             | Coluna normalizada           | Descrição resumida                                              |
|------------------------------------|------------------------------|-----------------------------------------------------------------|
| DATA_VENDA                         | `data_venda`                 | Data da venda que originou a devolução.                        |
| DATA_DEVOLUCAO                     | `data_devolucao`             | Data em que a devolução foi registrada.                        |
| NOTA_FISCAL_VENDA                  | `nr_nota_fiscal`             | Nota fiscal original vinculada à devolução.                    |
| NOTA_FISCAL_DEVOLUCAO              | `nr_nota_devolucao`          | Nota fiscal emitida para a devolução (quando houver).          |
| CATEGORIA                          | `categoria`                  | Segmento do SKU devolvido.                                     |
| CD_ANUNCIO                         | `cd_anuncio`                 | Código do anúncio associado à venda original (quando disponível). |
| DS_ANUNCIO                         | `ds_anuncio`                 | Descrição do anúncio associado (quando disponível).             |
| CD_PRODUTO                         | `cd_produto`                 | Código interno do SKU devolvido.                               |
| CD_FABRICANTE                      | `cd_fabricante`              | Código do fabricante ou parceiro.                              |
| DS_PRODUTO                         | `ds_produto`                 | Descrição do SKU devolvido.                                    |
| TP_ANUNCIO                         | `tp_anuncio`                 | Tipo de anúncio do item devolvido.                             |
| Unidades                           | `qtd_sku`                    | Quantidade devolvida.                                          |
| Devolução Receita Bruta Tot$       | `devolucao_receita_bruta`    | Valor bruto devolvido referente à nota.                        |
| Custo Medio$ / Custo Médio$        | `custo_produto`              | Custo unitário associado à devolução (quando informado).       |
| Preco Medio Unit$ / Preço Medio Unit$ | `preco_vendido`          | Preço unitário registrado na devolução.                        |
| TP_REGISTRO                        | `tp_registro`                | Identificador interno da planilha para linhas de devolução.    |

> **Importante:** os nomes das colunas são normalizados (minúsculo, sem espaços extras) e datas são interpretadas com `dayfirst=True`, garantindo que `07/01/2026` seja entendido como 7 de janeiro. O carregamento também cria colunas derivadas como `periodo` (`Period[M]`) e `ano_mes` (`YYYYMM`). Campos textuais recebem valores padrão quando vazios, preservando consistência ao filtrar por categoria, anúncio ou SKU. As devoluções ficam disponíveis em `df.attrs["returns_data"]`, permitindo cruzar a data da venda com a data da devolução em análises específicas.

---

## 5. Enriquecimento Automático de Dados


1. **Datas padronizadas**: a coluna `data` é convertida para `datetime` (interpretando `dd/mm/aaaa`) e normalizada para meia-noite. A partir dela são gerados `periodo` (`Period[M]`) e `ano_mes` (`YYYYMM`) para manter comparações mensais.
2. **Coerção numérica**: remove símbolos (`%`, vírgula decimal) e converte a `float`. Percentuais acima de 1 são ajustados para escala 0-1 (ex.: `25` vira `0.25`).
3. **Normalização de texto**: `categoria`, `cd_anuncio`, `ds_anuncio`, `cd_produto`, `ds_produto`, `cd_fabricante` e `tp_anuncio` são preenchidos com valores padrão e aparados.
4. **Métricas derivadas**:
   - `receita_bruta_calc = preco_vendido * qtd_sku`
   - `rbld = RBLD` quando informado; caso contrário usa `receita_bruta_calc`
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
3. Escolha a categoria (ou "Todas" para analisar o portfólio completo). Na análise de performance de venda você pode optar entre filtrar por categoria ou informar uma lista de `CD_ANUNCIO`.
4. Para análises que pedem ranking, defina o tamanho desejado (10, 20, 50, 100 ou outro valor positivo).
5. Na análise de potencial, personalize a janela recente se desejar (quantidade de meses e períodos específicos).
6. Aguarde a geração e anote o caminho do arquivo exibido no console.
7. Escolha "s" para continuar analisando ou "n" para encerrar.

---

## 7. Descrição das Análises

### 7.1 Análise de Devolução (`RETURN`)

**Objetivo**: enxergar devoluções sob duas perspectivas complementares — mês da venda original e mês em que a devolução foi registrada — sem distorcer a taxa de devolução mensal.

**Como funciona**:
- As abas de devolução são normalizadas para manter tanto a `DATA_VENDA` quanto a `DATA_DEVOLUCAO`, permitindo atrelar cada devolução ao mês correto da venda ou do retorno.
- Para cada linha de devolução é buscada a quantidade vendida original nas abas de venda utilizando a combinação `NOTA_FISCAL_VENDA` + `CD_PRODUTO`, garantindo que o denominador da taxa corresponda exatamente ao SKU devolvido.
- Para cada período são calculados: `itens_devolvidos`, `receita_devolucao`, `pedidos_devolvidos` (notas de devolução únicas) e `taxa_devolucao = itens_devolvidos / itens_vendidos`. As taxas são exibidas já formatadas em `%`.

**Relatórios gerados**:
- `Dev. atrelada ao mês da venda`: contabiliza a devolução no mês em que a venda ocorreu (`DATA_VENDA`). Útil para avaliar a qualidade comercial daquele período sem impacto de devoluções tardias.
- `Analise de Dev. mensal`: contabiliza a devolução no mês em que ela foi processada (`DATA_DEVOLUCAO`). Ajuda a entender picos operacionais de retorno e o impacto financeiro em cada mês de processamento.

Cada aba traz as colunas: `ano`, `mes_extenso`, `mes_abreviado`, `periodo` (`YYYY-MM`), `cd_produto`, `ds_produto`, `itens_vendidos`, `itens_devolvidos`, `pedidos_devolvidos`, `receita_devolucao` e `taxa_devolucao`. Os itens vendidos são obtidos diretamente das abas de venda combinando `NOTA_FISCAL_VENDA` + `CD_PRODUTO`, garantindo que a quantidade original corresponda exatamente ao SKU devolvido.


---

### 7.2 Análise de SKU em Potencial de Venda (`POTENTIAL`)

**Objetivo**: destacar anúncios (CD_ANUNCIO) que apresentavam histórico forte e caíram recentemente, mas ainda têm potencial de retomada.

**Processo**:
1. Agrupa vendas por mês e anúncio, calculando médias de `qtd_vendida`, `pedidos`, `receita`, `custo`, `margem`, `qtd_devolvida`.
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
- `skus_potenciais_mensal`: histórico apenas dos anúncios selecionados, trazendo o preço mínimo por período e referências históricas para entender a trajetória de cada item.

**Intuito**: evidenciar oportunidades para campanhas de reativação, ajustes de estoque ou revisão de posicionamento comercial.

> 💡 Durante a execução é possível definir manualmente a janela recente: informe a quantidade de meses a analisar e selecione exatamente quais períodos (ano/mês) serão comparados com o histórico.

---

### 7.3 Análise de Top SKUs Mais Vendidos Historicamente (`TOP_SELLERS`)

**Objetivo**: ranquear anúncios (CD_ANUNCIO) com melhor consistência ao longo do tempo.

**Passos**:
1. Agrupa por mês e anúncio: `qtd_vendida`, `pedidos`, `receita`, `devolucao`, `margem`.
2. Consolida por anúncio:
   - `meses_com_venda`, `quantidade_total`, `pedidos_total`, `receita_total`, `devolucao_total`, `margem_media`.
   - `taxa_devolucao_total = devolucao_total / quantidade_total`.
3. Filtra SKUs com pelo menos 3 meses com venda (`MIN_MONTHS_RECURRENCE`).
4. Ordena por `meses_com_venda`, `quantidade_total`, `receita_total` (todos decrescentes) e seleciona o ranking.
5. Calcula `ticket_medio_estimado = receita_total / pedidos_total`.

**Relatórios gerados**:
- `ranking`: tabela final com pedidos totais (notas distintas), taxa de devolução, margem média em `%`, ticket médio, preço médio do intervalo e referências de preço (`preco_min_intervalo`, `preco_min_historico_total`).
- `detalhe_mensal`: visão mensal dos anúncios ranqueados, incluindo margens em `%`, preço médio do mês e preços mínimos para acompanhar a competitividade.

**Intuito**: apoiar decisões de sortimento principal, planejamento de estoque e reconhecimento de best sellers sustentáveis.

---

### 7.4 Análise de Produto de Custo Baixo para Reputação (`REPUTATION`)

**Objetivo**: encontrar anúncios baratos que vendem bem e devolvem pouco, ideais para reforço de reputação, campanhas de entrada ou aumento de conversão.

**Metodologia**:
1. Agrupa por anúncio, calculando:
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

**Objetivo**: diagnosticar rapidamente o desempenho comercial dentro de um intervalo de datas, seja por categoria ou por uma lista específica de anúncios.

**Como funciona**:
- Após definir o período, escolha na CLI se deseja filtrar por uma categoria ou informar os `CD_ANUNCIO` de interesse.
- Quando a escolha for por categoria, todo o portfólio filtrado é avaliado; quando optar por `CD_ANUNCIO`, somente os anúncios informados são considerados (mesmo que pertençam a categorias distintas).
- A análise gera três visões complementares:
   - `resumo_produtos`: consolida o desempenho total no intervalo (receita, pedidos, margem média, ticket médio, devoluções, custo total, lucro estimado) junto com `cd_anuncio`, `ds_anuncio`, `cd_fabricante` e `tp_anuncio`.
   - `analise_diaria`: mostra a evolução dia a dia por anúncio, com métricas de pedidos, quantidade vendida, ticket médio, preços praticados e taxas de devolução.
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