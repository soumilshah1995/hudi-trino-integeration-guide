# hudi-trino-integeration-guide
hudi-trino-integeration-guide

![Screenshot 2024-05-09 at 11 47 08 AM](https://github.com/soumilshah1995/hudi-trino-integeration-guide/assets/39345855/1dd59542-1ff4-44ab-b254-ae7ce7d08945)


# Ingest Code 
```
try:
    import os
    import sys
    import uuid
    import pyspark
    import datetime
    from pyspark.sql import SparkSession
    from pyspark import SparkConf, SparkContext
    from faker import Faker
    import datetime
    from datetime import datetime
    from pyspark.sql.types import StructType, StructField, StringType

    import random
    import pandas as pd  # Import Pandas library for pretty printing

    print("Imports loaded ")

except Exception as e:
    print("error", e)

HUDI_VERSION = '1.0.0-beta1'
SPARK_VERSION = '3.4'

os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11"
SUBMIT_ARGS = f"--packages org.apache.hadoop:hadoop-aws:3.3.2,org.apache.hudi:hudi-spark{SPARK_VERSION}-bundle_2.12:{HUDI_VERSION} pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
os.environ['PYSPARK_PYTHON'] = sys.executable

# Spark session
spark = SparkSession.builder \
    .config('spark.executor.memory', '4g') \
    .config('spark.driver.memory', '4g') \
    .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
    .config('className', 'org.apache.hudi') \
    .config('spark.sql.hive.convertMetastoreParquet', 'false') \
    .getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://127.0.0.1:9000/")
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "admin")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "password")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider",
                                     "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

import uuid
from faker import Faker
from datetime import datetime

faker = Faker()

def get_customer_data(total_customers=2):
    customers_array = []
    for i in range(0, total_customers):
        customer_data = {
            "customer_id": str(uuid.uuid4()),
            "name": faker.name(),
            "created_at": datetime.now().isoformat().__str__(),
            "address": faker.address(),
            "state": str(faker.state_abbr()),  # Adding state information
            "salary": faker.random_int(min=30000, max=100000)
        }
        customers_array.append(customer_data)
    return customers_array

global total_customers, order_data_sample_size
total_customers = 10000
customer_data = get_customer_data(total_customers=total_customers)


from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Assuming you have the customer_data already defined

# Define schema
schema = StructType([
    StructField("customer_id", StringType(), nullable=True),
    StructField("name", StringType(), nullable=True),
    StructField("created_at", StringType(), nullable=True),  # Change to StringType
    StructField("address", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("salary", StringType(), nullable=True)
])

# Create DataFrame
spark_df_customers = spark.createDataFrame(data=customer_data, schema=schema)

# Show DataFrame
spark_df_customers.show(1, truncate=True)

# Print DataFrame schema
spark_df_customers.printSchema()
spark_df = spark_df_customers
database="default"
table_name="customers_t1"


path = f"s3a://warehouse/{database}/{table_name}"

hudi_options = {
    'hoodie.table.name': table_name,
    'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
    'hoodie.datasource.write.table.name': 'customers',
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.recordkey.field': 'customer_id',
    'hoodie.datasource.write.precombine.field': 'created_at',
    "hoodie.datasource.write.partitionpath.field": "state",


    "hoodie.enable.data.skipping": "true",
    "hoodie.metadata.enable": "true",
    "hoodie.metadata.index.column.stats.enable": "true",

    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    "hoodie.datasource.hive_sync.metastore.uris": "thrift://localhost:9083",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.database": "default",
    "hoodie.datasource.hive_sync.table": table_name,

}

print("\n")
print(path)
print("\n")

spark_df.write.format("hudi"). \
    options(**hudi_options). \
    mode("append"). \
    save(path)

```

```
%load_ext sql
%sql trino://admin@localhost:8080/default
%sql USE hudi.default
%sql select * from customers_t1 limit 2
```
