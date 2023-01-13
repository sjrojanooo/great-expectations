from pyspark.sql import SparkSession
import importlib 

adidas_transformations = importlib.import_module('etl.adidas_transformations')

def main():
    spark = SparkSession.builder.appName('AdidasDataQuality')\
                        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.HDFSLogStore") \
                        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
                        .enableHiveSupport()\
                        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    adidas_sales = spark.read.csv('./data/adidas_us_retail_sales_data-raw.csv', sep=',', header=True)
    adidas_sales = adidas_transformations.transform_columns(adidas_sales)
    adidas_sales = adidas_transformations.transform_literal_types(adidas_sales, ['price_per_unit', 'units_sold', 
                                                                                'total_sales', 'operating_profit', 
                                                                                'operating_margin'])
    adidas_sales = adidas_transformations.transform_datetime(adidas_sales)
    adidas_sales.toPandas().to_csv('./data/adidas_us_retail_sales_data-converted.csv', index=False)
if __name__ == '__main__':
    main()