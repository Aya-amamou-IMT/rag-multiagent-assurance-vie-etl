"""Extraction des documents bruts vers la couche Bronze.

Ce module ne transforme pas encore le contenu metier : il lit les fichiers,
conserve leur texte et ajoute des metadonnees de tracabilite.
"""

from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


BRONZE_SCHEMA = T.StructType(
    [
        T.StructField("document_id", T.StringType(), False),
        T.StructField("file_name", T.StringType(), False),
        T.StructField("file_path", T.StringType(), False),
        T.StructField("file_type", T.StringType(), False),
        T.StructField("raw_text", T.StringType(), True),
        T.StructField("ingestion_timestamp", T.TimestampType(), False),
        T.StructField("ingestion_status", T.StringType(), False),
        T.StructField("error_message", T.StringType(), True),
    ]
)


def extract_text_documents(spark: SparkSession, input_path: str) -> DataFrame:
    """Lit tous les fichiers .txt d'un dossier et retourne une couche Bronze.

    wholetext=True preserve un document complet par ligne. C'est pratique avant
    le futur decoupage en sections et en chunks.
    """
    return (
        spark.read.option("wholetext", True).text(input_path)
        .withColumn("file_path", F.input_file_name())
        .withColumn("file_name", F.element_at(F.split("file_path", r"/|\\"), -1))
        .withColumn("document_id", F.regexp_replace("file_name", r"\.txt$", ""))
        .withColumn("file_type", F.lit("txt"))
        .withColumnRenamed("value", "raw_text")
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("ingestion_status", F.lit("success"))
        .withColumn("error_message", F.lit(None).cast("string"))
        .select([field.name for field in BRONZE_SCHEMA])
    )


def _extract_pdf_partition(rows):
    """Extrait le texte PDF. PyPDF doit etre installe sur chaque executant Spark."""
    from pypdf import PdfReader

    timestamp = datetime.now(timezone.utc)
    for row in rows:
        path = row.path
        file_name = Path(path).name
        try:
            reader = PdfReader(BytesIO(row.content))
            raw_text = "\n".join(page.extract_text() or "" for page in reader.pages)
            status = "success" if raw_text.strip() else "empty_text"
            error = None
        except Exception as exc:  # Le pipeline conserve aussi les erreurs d'extraction.
            raw_text = None
            status = "failed"
            error = str(exc)

        yield (
            Path(file_name).stem,
            file_name,
            path,
            "pdf",
            raw_text,
            timestamp,
            status,
            error,
        )


def extract_pdf_documents(spark: SparkSession, input_path: str) -> DataFrame:
    """Lit les PDF avec Spark binaryFile puis extrait leur texte avec PyPDF."""
    binary_df = spark.read.format("binaryFile").load(input_path)
    return spark.createDataFrame(binary_df.select("path", "content").rdd.mapPartitions(_extract_pdf_partition), BRONZE_SCHEMA)


def extract_documents(spark: SparkSession, input_path: str, file_type: str) -> DataFrame:
    """Point d'entree unique pour l'extraction Bronze.

    Args:
        spark: session PySpark active.
        input_path: chemin du dossier ou des fichiers a lire.
        file_type: ``txt`` ou ``pdf``.
    """
    extractors = {"txt": extract_text_documents, "pdf": extract_pdf_documents}
    if file_type not in extractors:
        raise ValueError("file_type doit etre 'txt' ou 'pdf'.")
    return extractors[file_type](spark, input_path)


def write_bronze(df: DataFrame, output_path: str) -> None:
    """Ecrit la couche Bronze au format Parquet."""
    df.write.mode("overwrite").format("parquet").save(output_path)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("BankingDocumentExtraction").master("local[*]").getOrCreate()

    bronze_df = extract_documents(spark, "data/raw/*.txt", file_type="txt")
    bronze_df.show(truncate=False)
    write_bronze(bronze_df, "data/bronze/raw_documents")

    spark.stop()
