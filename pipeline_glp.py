from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, lit, coalesce, regexp_replace, to_date
from pyspark.sql.types import StringType, DoubleType

# 1. Criação da sessao Spark
spark = SparkSession.builder.master("local[*]").appName("TrabalhoFinal").getOrCreate()

# 2. Caminhos e formato definidos diretamente por variáveis para rodar no notebook
arquivo_entrada = "/content/glp-2025-01.csv"
arquivo_saida = "/content/resultado_parquet"
formato = "csv"

# 3. Ler o arquivo de entrada
if formato == "csv":
    df = spark.read \
        .option("header", "true") \
        .option("sep", ";") \
        .option("encoding", "UTF-8") \
        .option("inferSchema", "false") \
        .csv(arquivo_entrada)

    # Renomear as colunas para nomes sem espacos e sem hifens.
    df = df.toDF(
        "regiao_sigla",
        "estado_sigla",
        "municipio",
        "revenda",
        "cnpj_revenda",
        "nome_rua",
        "numero_rua",
        "complemento",
        "bairro",
        "cep",
        "produto",
        "data_coleta",
        "valor_venda",
        "valor_compra",
        "unidade_medida",
        "bandeira"
    )

elif formato == "parquet":
    df = spark.read.parquet(arquivo_entrada)

else:
    raise ValueError("Formato invalido")

# 4. Converter campos vazios ou com espacos para NULL
for coluna in df.columns:
    df = df.withColumn(
        coluna,
        when(trim(col(coluna)) == "", lit(None)).otherwise(trim(col(coluna)))
    )

# 5. Aplicar a tipagem correta
colunas_texto = [
    "regiao_sigla",
    "estado_sigla",
    "municipio",
    "revenda",
    "cnpj_revenda",
    "nome_rua",
    "numero_rua",
    "complemento",
    "bairro",
    "cep",
    "produto",
    "unidade_medida",
    "bandeira"
]

for coluna in colunas_texto:
    df = df.withColumn(coluna, col(coluna).cast(StringType()))

# Converter a data do formato brasileiro para Date
df = df.withColumn(
    "data_coleta",
    to_date(col("data_coleta"), "dd/MM/yyyy")
)

# Trocar a virgula por ponto e converter os precos para Double
df = df.withColumn(
    "valor_venda",
    regexp_replace(col("valor_venda"), ",", ".").cast(DoubleType())
)

df = df.withColumn(
    "valor_compra",
    regexp_replace(col("valor_compra"), ",", ".").cast(DoubleType())
)

# 6. Aplicar valores default onde faz sentido
for coluna in colunas_texto:
    df = df.withColumn(
        coluna,
        coalesce(col(coluna), lit("NAO INFORMADO"))
    )

df = df.withColumn(
    "data_coleta",
    coalesce(col("data_coleta"), to_date(lit("1900-01-01")))
)

df = df.withColumn(
    "valor_venda",
    coalesce(col("valor_venda"), lit(0.0))
)

# 7. Mostrar o resultado antes de gravar
print("Estrutura final:")
df.printSchema()

# 8. Salvar em Parquet, particionando por Estado
df.write.mode("overwrite").partitionBy("estado_sigla").parquet(arquivo_saida)

print("Arquivo Parquet salvo em:", arquivo_saida)

spark.stop()
