# Sales Insight Toolkit CLI

Ferramenta interativa em Python para explorar vendas históricas, devoluções e performance comercial a partir da planilha `BASE.xlsx`. Os relatórios são gerados em Excel (uma aba por visão) e organizados por análise, permitindo comparar períodos, categorias e anúncios com indicadores consistentes.

---

## Destaques

- Carregamento único com cache opcional em `.cache/` e barra de progresso percentuais reais.
- Normalização automática de colunas de venda e devolução, incluindo cálculo de métricas derivadas (`rbld`, margem, taxas, preços unitários).
- CLI interativa para escolher análise, período, categoria ou lista de anúncios, parâmetros de ranking e personalização da janela recente.
- Exportação padronizada para a pasta `output/` com planilhas formatadas como tabelas do Excel.

---

## Estrutura do Projeto

```
main.py                  # ponto de entrada da aplicação
analysis/
      __init__.py
      cli.py               # fluxo interativo, prompts e exportação
      data_loader.py       # leitura, normalização e cache do dataset
      exporters.py         # utilitário para gerar arquivos .xlsx
      formatting.py        # tratamento de colunas percentuais
      reporting/
            __init__.py
            common_returns.py  # helpers compartilhados para devoluções
            returns.py         # visão mensal de devoluções
            potential.py       # identificação de SKUs com queda recente
            top_history.py     # ranking histórico de recorrência
            low_cost.py        # produtos baratos com boa reputação
            product_focus.py   # perspectiva consolidada diária/mensal
output/
```

---

## Pré-requisitos

1. Python 3.10 ou superior instalado.
2. Ambiente virtual recomendado:

    ```powershell
    python -m venv .venv
    .\.venv\Scripts\Activate.ps1
    pip install pandas numpy openpyxl xlsxwriter
    ```

3. Planilha `BASE.xlsx` na raiz com abas `VENDA`, `VENDA01`, ... e, opcionalmente, `DEVOLUCAO`, `DEVOLUCAO01`, ... contendo as colunas abaixo.

---

## Dados de Entrada

### Aba de vendas (`VENDA`, `VENDA01`, ...)

| Coluna original (Excel)              | Coluna normalizada | Descrição resumida                                                |
|-------------------------------------|--------------------|-------------------------------------------------------------------|
| DATA_VENDA                          | `data`             | Data da venda (`dd/mm/aaaa`), usada para gerar `periodo` e `ano_mes`. |
| NOTA_FISCAL_VENDA                   | `nr_nota_fiscal`   | Identificador da nota/pedido.                                     |
| CATEGORIA                           | `categoria`        | Segmento ou linha de produto.                                     |
| CD_ANUNCIO                          | `cd_anuncio`       | Código do anúncio comercial.                                     |
| DS_ANUNCIO                          | `ds_anuncio`       | Descrição comercial do anúncio.                                   |
| CD_PRODUTO                          | `cd_produto`       | Código interno do SKU.                                            |
| DS_PRODUTO                          | `ds_produto`       | Nome do SKU.                                                      |
| CD_FABRICANTE                       | `cd_fabricante`    | Código do fabricante/parceiro.                                    |
| TP_ANUNCIO                          | `tp_anuncio`       | Tipo de anúncio (kit, variação, etc.).                            |
| Unidades                            | `qtd_sku`          | Quantidade vendida na linha da nota.                              |
| Preco Medio Unit$ / Preço Medio Unit$ | `preco_unitario` | Preço médio unitário informado na planilha.                       |
| Custo Medio$ / Custo Médio$         | `custo_produto`    | Custo médio unitário (valores somados posteriormente).            |
| Perc Margem Bruta% RBLD             | `perc_margem_bruta`| Margem bruta informada (normalizada para faixa 0–1).              |
| Receita Bruta (-) Devoluções Tot$   | `rbld`             | Receita líquida de devoluções, usada como base de preço unitário. |
| TP_REGISTRO                         | `tp_registro`      | Indicador interno (linhas não “venda” são ignoradas).             |

### Abas de devolução (`DEVOLUCAO`, `DEVOLUCAO01`, ...)

| Coluna original (Excel)              | Coluna normalizada        | Descrição resumida                                             |
|-------------------------------------|---------------------------|----------------------------------------------------------------|
| DATA_VENDA                          | `data_venda`              | Data da venda que originou a devolução.                         |
| DATA_DEVOLUCAO                      | `data_devolucao`          | Data em que a devolução foi processada.                        |
| NOTA_FISCAL_VENDA                   | `nr_nota_fiscal`          | Nota fiscal original vinculada ao retorno.                     |
| NOTA_FISCAL_DEVOLUCAO               | `nr_nota_devolucao`       | Nota fiscal da devolução (quando houver).                      |
| CATEGORIA                           | `categoria`               | Segmento do SKU devolvido.                                     |
| CD_ANUNCIO                          | `cd_anuncio`              | Código do anúncio devolvido (fallback para `cd_produto`).      |
| DS_ANUNCIO                          | `ds_anuncio`              | Descrição associada à devolução.                               |
| CD_PRODUTO                          | `cd_produto`              | Código interno do SKU devolvido.                               |
| CD_FABRICANTE                       | `cd_fabricante`           | Código do fabricante.                                          |
| DS_PRODUTO                          | `ds_produto`              | Nome do SKU devolvido.                                         |
| TP_ANUNCIO                          | `tp_anuncio`              | Tipo de anúncio devolvido.                                     |
| Unidades                            | `qtd_sku`                 | Quantidade devolvida.                                          |
| Devolução Receita Bruta Tot$        | `devolucao_receita_bruta` | Valor bruto devolvido.                                         |
| Custo Medio$ / Custo Médio$         | `custo_produto`           | Custo unitário informado na devolução.                         |
| Preco Medio Unit$ / Preço Medio Unit$ | `preco_unitario`       | Preço unitário registrado na devolução.                        |
| TP_REGISTRO                         | `tp_registro`             | Indicador interno (linhas não “devolução” são removidas).      |

> O carregador aceita múltiplas abas que compartilham o mesmo prefixo (`VENDA01`, `VENDA02`, ...) e consolida tudo em um único DataFrame.

---

## Processamento Automático do Carregador

- **Datas e períodos**: converte `data` para `datetime` (interpretação `dayfirst`), gera `periodo` (`Period[M]`) e `ano_mes` (`YYYYMM`).
- **Normalização textual**: remove espaços extras e substitui valores ausentes em `categoria`, `cd_anuncio`, `cd_produto`, `ds_anuncio`, `ds_produto`, `cd_fabricante`, `tp_anuncio` e `nr_nota_fiscal` por padrões seguros.
- **Coerção numérica**: limpa símbolos (`%`, vírgula decimal) e converte para `float`. Percentuais acima de 1 viram escala 0–1.
- **Métricas derivadas**:
   - `receita_bruta_calc = preco_unitario * qtd_sku`.
   - `rbld` recebe `receita_bruta_calc` quando o valor informado é vazio ou zero.
   - `lucro_bruto_estimado = receita_bruta_calc * perc_margem_bruta`.
   - `taxa_devolucao` calculada com os dados de devolução vinculados por nota e SKU.
- **Dados de devolução**: o merge adiciona `qtd_devolvido` e `devolucao_receita_bruta` ao DataFrame principal e salva o conjunto completo de devoluções em `df.attrs["returns_data"]` para uso nas análises.
- **Cache**: ao finalizar o carregamento, o dataset tratado é salvo em `.cache/<arquivo>_<assinatura>.pkl`. Se `BASE.xlsx` não mudar, a próxima execução reaproveita esse cache e pula a leitura do Excel.
- **Progresso**: a CLI mostra progresso percentual real durante a leitura das abas e exibe um spinner dedicado durante o cálculo das métricas históricas.

---

## Executando a Aplicação

```powershell
python main.py
```

Fluxo interativo:

1. Selecione a análise desejada.
2. Visualize os períodos disponíveis e informe as datas inicial/final (`DD/MM/AAAA`) ou pressione Enter para considerar todo o histórico.
3. Filtre por categoria (todas ou uma específica). Na análise de performance (`PRODUCT_FOCUS`) é possível optar por informar manualmente uma lista de `CD_ANUNCIO` em vez da categoria.
4. Informe parâmetros adicionais quando solicitados:
    - Tamanho do ranking (`POTENTIAL`, `TOP_SELLERS`).
    - Janela recente personalizada (`POTENTIAL`).
5. Aguarde a geração do arquivo Excel (o caminho completo é exibido no final). Todos os relatórios são salvos em `output/` com timestamp no nome.
6. Escolha executar outra análise ou encerrar.

---

## Análises Disponíveis

### 1. Devoluções (`RETURN`)

- **Objetivo**: comparar devoluções pelo mês da venda original e pelo mês em que a devolução ocorreu, sem distorcer o denominador de itens vendidos.
- **Como calcula**:
   - Normaliza a base de devoluções, garantindo `pedido_devolucao_id` e `periodo_venda`/`periodo_devolucao`.
   - Cruza devoluções com o volume vendido (`cd_produto` + `periodo`) para medir taxas consistentes.
- **Planilhas geradas**:
   - `Dev. atrelada ao mês da venda`: devolução contabilizada no mês da venda original.
   - `Analise de Dev. mensal`: devolução contabilizada no mês em que foi processada.
- **Colunas chave**: `ano`, `mes_extenso`, `mes_abreviado`, `periodo`, `cd_produto`, `ds_produto`, `itens_vendidos`, `itens_devolvidos`, `pedidos_devolvidos`, `receita_devolucao`, `taxa_devolucao`.

### 2. SKU em Potencial (`POTENTIAL`)

- **Objetivo**: destacar anúncios com histórico sólido que sofreram queda recente e podem ser reativados.
- **Como calcula**:
   - Divide o histórico entre janela recente (últimos meses definidos pela CLI) e período histórico.
   - Calcula médias de quantidade, receita, pedidos, devolução, margem e preço mínimo (`_preco_rbld`).
   - Aplica filtros mínimos: pelo menos três meses históricos, volume histórico acima da mediana, queda percentual >= 30% e taxa de devolução recente <= 20%.
   - Classifica pelo `potencial_score`, que combina queda absoluta, meses válidos e taxa de devolução histórica.
- **Planilhas geradas**:
   - `potenciais`: ranking final com `categoria`, `cd_produto`, `cd_anuncio`, `queda_abs_qtd`, `queda_pct_qtd`, `potencial_score`, métricas históricas e recentes, preços mínimos do intervalo e do histórico completo.
   - `skus_potenciais_mensal`: histórico mensal dos SKUs selecionados com `preco_medio_vendido`, `preco_min_periodo`, devoluções e margens.

### 3. Top SKUs Históricos (`TOP_SELLERS`)

- **Objetivo**: ranquear anúncios com maior consistência de venda ao longo do tempo.
- **Como calcula**:
   - Soma pedidos, quantidade, receita e devoluções por anúncio.
   - Impõe recorrência mínima de três meses com venda.
   - Calcula ticket médio, preço médio do intervalo e taxas usando `rbld` dividido por quantidade.
   - Integra dados de devolução por SKU para trazer devolução total, pedidos devolvidos e receita devolvida.
- **Planilhas geradas**:
   - `ranking`: top N (configurável) com `categoria`, `cd_anuncio`, `cd_produto`, `meses_com_venda`, `quantidade_total`, `pedidos_total`, `receita_total`, margens, devoluções, tickets e preços mínimos (intervalo e histórico completo).
   - `detalhe_mensal`: evolução mensal dos SKUs ranqueados com `ano`, `mes_abrev`, `preco_medio_unitario_vendido`, `preco_min_periodo`, devoluções e preços de referência.

### 4. Produtos de Baixo Custo para Reputação (`REPUTATION`)

- **Objetivo**: encontrar anúncios baratos, com giro e baixa devolução para reforço de reputação ou campanhas de entrada.
- **Como calcula**:
   - Agrega itens por anúncio e ordena pelo percentil de custo unitário (`cost_percentile`, padrão 25%).
   - Filtra por quantidade mínima (`min_quantity`, padrão 50) e taxa de devolução máxima (`max_return_rate`, padrão 5%).
   - Calcula `potencial_reputacao_score = ((1 - taxa_devolucao) * itens_vendidos_total) / custo_medio_unitario`.
- **Planilha gerada**:
   - `produtos_indicados`: lista com `categoria`, `cd_anuncio`, `cd_produto`, volumes, pedidos, receita, custos, margens, devoluções, ticket médio e preços mínimos (intervalo e histórico total).

### 5. Performance Focada por Produto (`PRODUCT_FOCUS`)

- **Objetivo**: diagnosticar rapidamente a performance comercial em um intervalo, filtrando por categoria ou lista de anúncios específicos.
- **Como calcula**:
   - Normaliza período e data, agrupa por diferentes granularidades (total, diário e mensal).
   - Métricas centralizadas: `qtd_pedidos`, `itens_vendidos`, `receita`, `ticket_medio`, `preco_medio_vendido_unitario`, `preco_medio_praticado_unitario` (derivado de `rbld`), `preco_min_unitario_periodo`, `margem_media`, `lucro_bruto_estimado`, `custo_produto` e devoluções vinculadas.
- **Planilhas geradas**:
   - `resumo_produtos`: visão consolidada por anúncio com dados de fabricante, tipo de anúncio e categoria.
   - `analise_diaria`: evolução diária com preços praticados e devoluções.
   - `analise_mensal`: agregação mensal adicionando `ano` e `mes_abrev` para leitura rápida.

Todas as análises que utilizam preços mínimos recebem as colunas `preco_min_unitario_intervalo` (menor preço no recorte analisado) e `preco_min_unitario_historico_total` (menor preço da série completa), calculadas a partir do mapa gerado no carregamento inicial.

---

## Personalização

- Ajuste os parâmetros padrão diretamente nos módulos em `analysis/reporting/` (`RECENT_WINDOW`, `MIN_DROP_RATIO`, `COST_PERCENTILE`, `MIN_MONTHS_RECURRENCE`, etc.).
- Para alterar filtros ou colunas exportadas, edite os `DataFrame` construídos nas funções `build_*_analysis` correspondentes.
- O comportamento do cache pode ser ajustado passando `enable_cache=False` ou um diretório diferente ao chamar `load_sales_dataset`.

---

## Boas Práticas

- Mantenha a planilha de origem alinhada às colunas esperadas e utilize abas sequenciais quando precisar fracionar a base.
- Revise os resultados com as equipes comercial e operacional para validar critérios de corte e thresholds.
- Considere adicionar testes automatizados ao introduzir novas regras ou análises para garantir consistência futura.

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
   - `custo_produto` já representa o custo total pelos itens na venda
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
   - `quantidade_total`, `pedidos_total`, `receita_total`, `custo_medio_unitario`, `custo_produto`, `devolucao_total`, `receita_devolucao_total`, `margem_media`.
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

## 9. Interface Conversacional (Streamlit + LangChain)

- **Estrutura**: todo o front-end fica em `streamlit_app/`. Lá estão o aplicativo (`app.py`), a camada de autenticação (`auth/`), os comandos do chatbot (`chat/`), os serviços de histórico e análises (`services/`), os utilitários de dados (`data/`) e a integração com LangChain (`langchain/`). Usuários e conversas são persistidos em SQLite (`SALES_TOOLKIT_DB`, padrão `.cache/streamlit/chatbot.db`).
- **Dependências**: instale os pacotes extras com `pip install -r requirements.txt`. O arquivo inclui `streamlit`, `langchain`, `langchain-community`, `langchain-openai`, `faiss-cpu` e `pyarrow` para vetorização e otimização do dataset.
- **Variáveis de ambiente principais**:
   - `SALES_TOOLKIT_ROOT`: raiz do projeto (padrão diretório atual).
   - `SALES_TOOLKIT_DATASET`: caminho absoluto do `BASE.xlsx` caso esteja fora da raiz.
   - `SALES_TOOLKIT_CACHE`: diretório de cache (default `.cache/streamlit`).
   - `SALES_TOOLKIT_DB`: caminho do SQLite com contas e histórico.
   - `SALES_TOOLKIT_VECTORSTORE`: pasta onde o índice FAISS é salvo.
   - `SALES_TOOLKIT_ADMIN_USER` / `SALES_TOOLKIT_ADMIN_PASSWORD`: criam o usuário padrão na inicialização.
   - `GEMINI_API_KEY`: chave usada por padrão para o provedor Gemini. Outras chaves (`OPENAI_API_KEY`, `AZURE_OPENAI_KEY`, etc.) podem ser definidas ao trocar o provedor nas configurações opcionais da `LangChainFactory` (`streamlit_app.llm.factory`).
- **Execução**: na raiz, rode `streamlit run streamlit_app/app.py`. Após o login, utilize `/ajuda` para listar comandos (`/analise_devolucao`, `/analise_potencial`, `/analise_top`, `/analise_reputacao`, `/analise_focus`). Comandos geram as mesmas tabelas da CLI, exibidas em abas com opção de download em Excel.
- **Fluxo da IA**: perguntas livres usam uma cadeia `RetrievalQA` alimentada por uma vector store FAISS construída sobre agregações de vendas por anúncio. A vectorização considera os itens de maior receita (até 5.000 documentos) mantendo o processamento viável para bases com milhões de linhas.
- **Otimização de dados**: a barra lateral permite gerar uma versão Parquet da base (`DatasetManager.to_parquet`) e recriar o índice de embeddings quando novos dados forem carregados.

---

## 10. Boas Práticas e Próximos Passos

- Revisar periodicamente se a base Excel está aderente às colunas esperadas.
- Validar os relatórios gerados com as equipes comercial e de operações.
- Incorporar testes automatizados à medida que novas regras de negócio forem adicionadas.