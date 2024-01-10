# Databricks notebook source
def mounting_layer_json(source,mountpoint,key,value):
    dbutils.fs.mount(
    source=source,
    mount_point= mountpoint,
    extra_configs={key:value})
 

# COMMAND ----------

# mounting_layer_json('wasbs://salesviewdevtst@adlstogen2.blob.core.windows.net/','/mnt/mountpointstorageadf','fs.azure.account.key.adlstogen2.blob.core.windows.net','D2RGMipb09LJoDXbNmHeBUrfCva/Zm/hO+qhcjkgRhFVfEvTmk2aaPErH31ChGTJorsWbtk3c222+ASt5WtDDw==')

# COMMAND ----------

#

# COMMAND ----------

# MAGIC %fs ls
# MAGIC '/mnt/mountpointstorageadf'

# COMMAND ----------

# MAGIC %fs ls
# MAGIC dbfs:/mnt/mountpointstorageadf/customer/

# COMMAND ----------

options={'header':True,
         'inferschema':True,
         'delimiter':','}
 
def read_csv(format,options,path):
    df=spark.read.format(format).options(**options).load(path)
    return df

# COMMAND ----------

df=read_csv('csv',options,'dbfs:/mnt/mountpointstorageadf/customer/20240105_sales_customer.csv')

# COMMAND ----------

def snake_case(x):
    a = x.lower()
    return a.replace(' ','_')

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
a=udf(snake_case,StringType())
lst = list(map(lambda x:snake_case(x),df.columns))
df1 = df.toDF(*lst)

# COMMAND ----------

df1.display()

# COMMAND ----------

df_final = df1.withColumn("first_name", split(col("name"), " ")[0]).withColumn("last_name", split(col("name"), " ")[1]).drop("name")

df_final.display()

# COMMAND ----------

df2 = df_final.withColumn("domain", split(col("email_id"), "@")[1])
df2.display()

# COMMAND ----------

df3=df2.withColumn("domain",split(col('domain'),"\.")[0])
df3.display()

# COMMAND ----------

a = 'wingacademy.com'
print(a.split('.')[0])

# COMMAND ----------

df3.groupBy('gender').count().display()

# COMMAND ----------

df4=df3.withColumn('gender',when(col("gender")=='male','M').
                   when(col('gender')=='female','F').
                   otherwise(col('gender')))
df4.display()

# COMMAND ----------

df5 = df4.withColumn("date", split(col("joining_date"), " ")[0]).withColumn("time", split(col("joining_date"), " ")[1])
df5.display()

# COMMAND ----------

df6 = df5.withColumn("date",date_format(to_date(col('date'),'dd-MM-yyyy'),'yyyy-MM-dd'))
df6.display()

# COMMAND ----------

df_1=df6.withColumn('expenditure-status',when(col('spent')<200,'minimum').otherwise('maximum'))
df_1.display()

# COMMAND ----------

df_1.filter(col('spent')<200).display()

# COMMAND ----------

df_1.write.option('header',True).format('csv').save('dbfs:/mnt/mountpointstorageadf/silver')

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/mountpointstorageadf/product/

# COMMAND ----------

dff1=read_csv('csv',options,'dbfs:/mnt/mountpointstorageadf/product/20240105_sales_product.csv')

# COMMAND ----------

dff1.display()

# COMMAND ----------

dff2=dff1.withColumn('sub_category',when(col("category_id")==1,'category_id').
                   when(col('category_id')==2,'phone').
                   when(col('category_id')==3,'laptop').
                   when(col('category_id')==4,'playstation').
                   otherwise(col('category_id')))
dff2.display()

# COMMAND ----------


