# Databricks notebook source
dbutils.fs.ls("/Volumes/workspace/default/darshan/")

# COMMAND ----------

df=spark.read.format("csv").option('inferSchema', True).option('header',True).load("/Volumes/workspace/default/darshan/BigMart Sales.csv")

# COMMAND ----------

df.show(5)

# COMMAND ----------

df.display()


# COMMAND ----------

df.printSchema()

# COMMAND ----------


my_ddl_schema = '''
                    Item_Identifier STRING,
                    Item_Weight STRING,
                    Item_Fat_Content STRING, 
                    Item_Visibility DOUBLE,
                    Item_Type STRING,
                    Item_MRP DOUBLE,
                    Outlet_Identifier STRING,
                    Outlet_Establishment_Year INT,
                    Outlet_Size STRING,
                    Outlet_Location_Type STRING, 
                    Outlet_Type STRING,
                    Item_Outlet_Sales DOUBLE 
                   '''


# COMMAND ----------

df = spark.read.format('csv')\
            .schema(my_ddl_schema)\
            .option('header',True)\
            .load('/FileStore/tables/BigMart_Sales.csv') 

# COMMAND ----------


from pyspark.sql.types import * 
from pyspark.sql.functions import *

# COMMAND ----------

my_strct_schema = StructType(
    [ StructField('Item_Identifier',StringType(),True),
      StructField('Item_Weight',StringType(),True), 
      StructField('Item_Fat_Content',StringType(),True), 
      StructField('Item_Visibility',StringType(),True), 
      StructField('Item_MRP',StringType(),True), 
      StructField('Outlet_Identifier',StringType(),True), 
      StructField('Outlet_Establishment_Year',StringType(),True),
       StructField('Outlet_Size',StringType(),True), 
       StructField('Outlet_Location_Type',StringType(),True), 
       StructField('Outlet_Type',StringType(),True),
        StructField('Item_Outlet_Sales',StringType(),True)

])

# COMMAND ----------

df = spark.read.format('csv').schema(my_strct_schema).option('header',True).load('/Volumes/workspace/default/darshan/BigMart Sales.csv')

# COMMAND ----------

df.select(col("Item_Identifier"),col("Item_Weight"),col("Item_Fat_Content")).display()

# COMMAND ----------

df.select(col("Item_Identifier").alias("item_id")).display()

# COMMAND ----------

df.filter(col("Item_Fat_Content")=='Regular').display()


# COMMAND ----------

df.filter((col('Item_MRP') == 'Soft Drinks') & (col('Item_Weight') < 10.0)).display()

# COMMAND ----------

df.filter((col('Outlet_Location_Type').isNull()) & (col('Outlet_Type').isin(['Tier 1','Tier 2']))).display()

# COMMAND ----------

df.withColumnRenamed('Item_Weight','Item_Wt').display()

# COMMAND ----------

df=df.withColumn('flag',lit("new"))

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('multiply',col('Item_Weight')).display()

# COMMAND ----------

df.withColumn("Item_Fat_Content",regexp_replace(col("Item_Fat_Content"), "Regular", "Reg"))\
    .withColumn("Item_Fat_Content",regexp_replace(col("Item_Fat_Content"), "Low Fat","LF")).display()


# COMMAND ----------

df.withColumn("Item_Weight",col("Item_Weight").cast(StringType()))

# COMMAND ----------

df.sort(col("Item_Weight").desc())\
    .sort(col("Item_Visibility").asc()).display()

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

df.drop("Item_Visibility").display()

# COMMAND ----------

