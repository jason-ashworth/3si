import sys

from functools import reduce
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.types import StringType


def main():
    try:
        # Initialize Spark session (Databricks provides this automatically)
        spark = SparkSession.builder.appName("Dataset Merge & Summary").getOrCreate()

        # List of dataset file paths (update with actual locations)
        dataset_paths = {
            "clickstream-enwiki-2020-01.tsv.gz": "https://3si-recruiting-tests.s3-us-west-2.amazonaws.com/clickstream-enwiki-2020-01.tsv.gz",
            "clickstream-enwiki-2020-02.tsv.gz": "https://3si-recruiting-tests.s3-us-west-2.amazonaws.com/clickstream-enwiki-2020-02.tsv.gz",
            "clickstream-enwiki-2020-03.tsv.gz": "https://3si-recruiting-tests.s3-us-west-2.amazonaws.com/clickstream-enwiki-2020-03.tsv.gz",
            "clickstream-enwiki-2020-04.tsv.gz": "https://3si-recruiting-tests.s3-us-west-2.amazonaws.com/clickstream-enwiki-2020-04.tsv.gz",
            "clickstream-enwiki-2020-05.tsv.gz": "https://3si-recruiting-tests.s3-us-west-2.amazonaws.com/clickstream-enwiki-2020-05.tsv.gz",
            "clickstream-enwiki-2020-06.tsv.gz": "https://3si-recruiting-tests.s3-us-west-2.amazonaws.com/clickstream-enwiki-2020-06.tsv.gz"
        }

        dataframes = []
        for dataset, url in dataset_paths.items():
            spark.sparkContext.addFile(url)
            df = spark.read.option("sep", "\t").option("compression", "gzip").option("header", False).csv("file://"+SparkFiles.get(dataset), header=False, inferSchema= True)
            df.select("_c0", "_c1", "_c2", "_c3")
            old_cols = ["_c0", "_c1", "_c2", "_c3"]
            new_cols = ["prev", "curr", "type", "occurrences"]
            df = reduce(lambda df, idx: df.withColumnRenamed(old_cols[idx], new_cols[idx]), range(len(old_cols)), df)
            df = df.orderBy(col("occurrences").desc()).limit(50)
            dataframes.append(df)
        
        merged_df = dataframes[0]
        for df in dataframes[1:]:
            casted_df = df
            for column in df.columns:
                casted_df = casted_df.withColumn(column, casted_df[column].cast(StringType()))
            merged_df = merged_df.union(casted_df)
        
        merged_df = merged_df.orderBy(col("occurrences").desc()).limit(50)

        merged_df.show(50)
        
        spark.stop()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        line_number = exc_tb.tb_lineno
        print(f"Exception occurred on line: {line_number}")
        print(e)

if __name__ == "__main__":
    main()