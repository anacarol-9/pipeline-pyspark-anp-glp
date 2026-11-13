# Pipeline de dados com PySpark: preços de GLP (ANP)

Pipeline em PySpark que lê a pesquisa de preços de GLP (gás de cozinha) da ANP, publicada no portal Dados Abertos (dados.gov.br), trata os dados e grava o resultado em Parquet particionado por estado.

Projeto individual do MBA em Engenharia de Dados da UFRJ (disciplina de Hadoop e Spark).

## Pergunta que o pipeline prepara

A base bruta vem num CSV com separador `;`, preços com vírgula decimal, datas no formato brasileiro e campos vazios. O objetivo é entregar uma base limpa e tipada, pronta para análise de preço por estado, município, revenda e bandeira.

## Dados

- **Fonte:** Série Histórica de Preços de Combustíveis e de GLP, ANP (dados.gov.br)
- **Arquivo usado:** GLP, janeiro de 2025 (`glp-2025-01.csv`)
- **Colunas:** região, estado, município, revenda, CNPJ, endereço, produto, data da coleta, valor de venda, valor de compra, unidade de medida e bandeira

Os dados não estão no repositório. Baixe o arquivo direto no portal da ANP.

## Etapas do pipeline

1. **Leitura** do CSV (ou de um Parquet) com cabeçalho e separador `;`, sem inferência de tipos.
2. **Padronização dos nomes** das colunas, sem espaços nem hífens.
3. **Vazios para nulo:** remove espaços nas pontas e converte textos vazios em `NULL`.
4. **Tipagem:**
    - textos como `String`, incluindo CNPJ e CEP, para não perder zeros à esquerda;
    - data da coleta convertida de `dd/MM/yyyy` para `Date`;
    - valores de venda e compra com a vírgula trocada por ponto e convertidos para `Double`.
5. **Valores padrão:**
    - textos nulos viram `NAO INFORMADO`;
    - data nula vira `1900-01-01`, uma data sentinela fácil de filtrar;
    - valor de venda nulo vira `0.0`.
6. **Gravação em Parquet**, particionado por `estado_sigla`.

## Decisões

- **Particionar por estado:** a maior parte das análises de preço filtra por UF. Com a partição, uma consulta de um estado lê só a pasta daquele estado.
- **CNPJ e CEP como texto:** são identificadores, não números. Como número, perderiam os zeros à esquerda.

## Como executar

Requer Python 3 e PySpark. Ajuste os caminhos de entrada e saída no início do script e rode:

```bash
spark-submit pipeline_glp.py
```

## Tecnologias

Python, PySpark (Spark SQL e DataFrames), Parquet.
