# coding: utf-8

# In[75]:


import os


# In[76]:


from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, col
from mlrun import get_or_create_ctx

mlrun_ctx=get_or_create_ctx('sparkjob')
# Initiate a new Spark Session
spark = SparkSession.builder.appName("Spark Session with Default Configurations").getOrCreate()

# Retrieve and view all the default Spark configurations:
# conf = spark.sparkContext._conf.getAll()
# print(conf)
conf = spark.sparkContext._conf


# In[77]:


# Relative path to the NoSQL table within the parent platform data container
table = os.path.join(os.getenv("V3IO_HOME_URL"), "examples", "bank")


# In[72]:


df2 = spark.read.format("io.iguaz.v3io.spark.sql.kv").load(table)


# In[73]:


# Relative path to the NoSQL table within the parent platform data container
parquet_table = os.path.join(os.getenv("V3IO_HOME_URL"), "examples", "bank_parquet")


# In[74]:


parqFile = os.path.join(parquet_table)

df2.write    .mode("overwrite")    .parquet(parqFile)


# In[ ]:


mlrun_ctx.log_dataset('bankxact',df=df2,format=csv)


# In[ ]:


spark.stop()


# In[ ]:


#jupyter nbconvert --to nuclio.export.NuclioExporter 
#'--output', yaml_path,

