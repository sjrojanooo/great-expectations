from pyspark.sql import SparkSession
import importlib 

adidas_transformations = importlib.import_module('etl.adidas_transformations')

def main():
    
    spark = SparkSession.builder.appName('AdidasDataQuality')\
                        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.HDFSLogStore") \
                        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
                        .enableHiveSupport()\
                        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # commenting out transformed processes as this was just needed to provide testing data for great expectations tutorial
    # feel free to comment out the code and run the spark submit command via docker cli
    
    # adidas_sales = spark.read.csv('./data/adidas_us_retail_sales_data-raw.csv', sep=',', header=True)
    # adidas_sales = adidas_transformations.transform_columns(adidas_sales)
    # adidas_sales = adidas_transformations.transform_literal_types(adidas_sales, ['price_per_unit', 'units_sold', 
    #                                                                             'total_sales', 'operating_profit', 
    #                                                                             'operating_margin'])
    # adidas_sales = adidas_transformations.transform_datetime(spark, adidas_sales)
    # adidas_sales.toPandas().to_csv('./data/adidas_us_retail_sales_data-converted.csv')

if __name__ == '__main__':
    main()