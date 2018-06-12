from pyspark.sql import SparkSession, Window
from pyspark.conf import SparkConf
import pyspark.sql.functions as f


def quarterly_asset_change(w):
    return (f.col('TOTAL ASSETS') - f.lag('TOTAL ASSETS', 1).over(w)) / f.lag('TOTAL ASSETS', 1).over(w)


if __name__ == '__main__':
    spark = SparkSession.builder \
                        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.2.2') \
                        .getOrCreate()

    banks_df = spark.read.format('com.mongodb.spark.sql.DefaultSource').option('uri',
            'mongodb://127.0.0.1:27017/precision_lender.banks').load()

    banks_df = banks_df.select('IDRSSD', 'Financial Institution Name').distinct()

    balance_sheet_df = spark.read.format('com.mongodb.spark.sql.DefaultSource').option('uri',
            'mongodb://127.0.0.1:27017/precision_lender.balance_sheet').load()
    w = Window().partitionBy('IDRSSD').orderBy('Reporting Period')

    average_quarterly_asset_growth = balance_sheet_df \
        .withColumn('Quarterly Percent Change',
                    quarterly_asset_change(w)) \
        .groupBy('IDRSSD').agg(f.mean('Quarterly Percent Change').alias('Mean Quarterly Percent Change'))

    df = banks_df.join(average_quarterly_asset_growth, on=['IDRSSD']) \
                 .select('Financial Institution Name', 'Mean Quarterly Percent Change') \
                 .sort(f.col('Mean Quarterly Percent Change').desc())
    df.show(truncate=False)
