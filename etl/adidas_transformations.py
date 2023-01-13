from pyspark.sql import DataFrame, Window, SparkSession
from pyspark.sql import functions as F
import re

# rename all columns and substitue and an underscore in between words an white space
def transform_columns(adidas_df: DataFrame):
    for column in adidas_df.columns:
        rename_column = re.sub('(\\s+)', '_', column.lower())
        adidas_df = adidas_df.withColumnRenamed(column, rename_column.strip())
    return adidas_df

# replace all literal type characters and replace them with an empty string
def transform_literal_types(adidas_df: DataFrame, list_o_columns: list) -> DataFrame:
    for column in list_o_columns: 
        adidas_df = adidas_df.withColumn(column, F.regexp_replace(column, '([$,%])', '').cast('double'))
    return adidas_df   

# transform invoice date to to a datetime column type 
def transform_datetime(spark: SparkSession, input_df: DataFrame) -> DataFrame: 
    input_df.createOrReplaceTempView('tmp_df')
    list_o_columns = ','.join([column for column in input_df.columns if column != 'invoice_date'])
    output_df = spark.sql(f"""
                        SELECT to_date(invoice_date, 'M/d/yyyy') as invoice_date, {list_o_columns}
                        FROM tmp_df;
                        """)
    spark.catalog.dropTempView('tmp_df')
    return output_df

# aggregate metric for adidas retails 
def min_max_and_datediff(spark: SparkSession, input_df: DataFrame) -> DataFrame:
    input_df.createOrReplaceTempView('tmp_df')
    output_df = spark.sql(f"""
                        SELECT 
                            retailer, retailer_id, region, 
                            MIN(invoice_date) as min_date, MAX(invoice_date) as max_date, 
                            datediff(MAX(invoice_date), MIN(invoice_date)) as day_in_sales, 
                            SUM(unit_sold) as total_units_sold, 
                            SUM(total_sales) as total_sales, 
                            SUM(operating_profit) as total_operating_profit
                        FROM tmp_df
                        GROUP BY retailer, retailer_id, region 
                        ORDER BY day_in_sales DESC;
                        """)
    spark.catalog.dropTempView('tmp_df')
    return output_df 

# get the percentage of each that each retailer takes up. 
def sum_values_by_region(spark: SparkSession, retail_summary: DataFrame) -> DataFrame: 
    retail_summary.createOrReplaceTempView('tmp_df')
    retail_summary = spark.sql(f"""
            WITH regions_sums AS (
                SELECT *, 
                SUM(total_units_sold) OVER (PARTITIONED BY region) as total_units_sold_by_region, 
                SUM(total_sales) OVER (PARTITIONED BY region) as total_sales_by_region, 
                SUM(total_operating_profit) OVER (PARTITIONED BY region) as total_operating_profit_by_region
                FROM tmp_df
            ),
            final_summary (
                SELECT
                *,
                Round(total_units_sold/total_units_sold_by_region) as percentage_of_total_units_sold_by_region,
                Round(total_sales/total_sales_by_region) as percentage_of_total_sales_by_region,
                Round(total_operating_profit_by_region/total_operating_profit_by_region) as percentage_of_total_operating_profit_by_region
                FROM regions_sums
            )
            SELECT * FROM final_summary
    """)
    spark.catalog.dropTempView('tmp_df')
    return retail_summary