import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_timestamp, when, regexp_replace, substring
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, DecimalType, BooleanType, LongType
)
from pyspark.sql import SQLContext


def generate_token(cluster_endpoint, region):
    """Generate IAM authentication token for Aurora DSQL."""
    client = boto3.client("dsql", region_name=region)
    token = client.generate_db_connect_admin_auth_token(
        cluster_endpoint,
        Region=region,
        ExpiresIn=3600
    )
    print("Auth token generated successfully")
    return token


# Initialize the Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'table_name'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# =============================================================================
# CONFIGURATION - Update these values for your environment
# =============================================================================
your_cluster_endpoint = "YOUR_CLUSTER_ENDPOINT.dsql.REGION.on.aws"
region = "YOUR_REGION"
username = "admin"

# Parameters
table_name = args['table_name']
drop_existing_table = False

# Generate auth token
auth_token = generate_token(your_cluster_endpoint, region)

# Database connection properties
db_properties = {
    "url": f"jdbc:postgresql://{your_cluster_endpoint}:5432/postgres",
    "user": username,
    "password": auth_token,
    "driver": "org.postgresql.Driver",
    "ssl": "true",
    "sslmode": "require"
}

# Get the S3 path and schema from the Glue Data Catalog
glue_client = boto3.client('glue')
response = glue_client.get_table(DatabaseName='YOUR_CATALOG_DATABASE', Name=table_name)
s3_path = response['Table']['StorageDescriptor']['Location']
glue_schema = response['Table']['StorageDescriptor']['Columns']

# Filter out the 'op' column (added by DMS for full load operations)
filtered_glue_schema = [col for col in glue_schema if col['Name'].lower() != 'op']

# Print the schema for debugging
print("Filtered Glue Schema:")
for column in filtered_glue_schema:
    print(f"Column: {column['Name']}, Type: {column['Type']}")

# =============================================================================
# DATA TYPE MAPPING
# =============================================================================
data_type_mapping = {
    "string": "VARCHAR(255)",
    "int": "INTEGER",
    "bigint": "BIGINT",
    "double": "DOUBLE PRECISION",
    "float": "REAL",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP",
    "date": "DATE",
    "decimal": "NUMERIC",
    "long": "BIGINT",
    "binary": "BYTEA",
    "char": "CHAR",
    "varchar": "VARCHAR",
    "array": "TEXT[]",
    "map": "JSONB",
    "struct": "JSONB",
    "small": "SMALLINT"
}


def get_spark_type(glue_type, column_name):
    """Map Glue Data Catalog types to PySpark types."""
    glue_type = glue_type.lower()

    # Special handling for ID columns while maintaining correct types
    if 'id' in column_name.lower():
        if glue_type.startswith('decimal') or glue_type in ['int', 'integer', 'bigint', 'long', 'numeric']:
            return LongType()
        elif glue_type.startswith('varchar') or glue_type == 'string':
            return StringType()
        elif glue_type.startswith('char'):
            return StringType()
        else:
            return StringType()

    # Normal type handling for non-ID columns
    if glue_type.startswith('decimal'):
        parts = glue_type.replace('decimal(', '').replace(')', '').split(',')
        precision = int(parts[0])
        scale = int(parts[1]) if len(parts) > 1 else 0
        return DecimalType(precision, scale)
    elif glue_type in ['int', 'integer']:
        return IntegerType()
    elif glue_type in ['long', 'bigint']:
        return LongType()
    elif glue_type in ['double', 'float']:
        return DoubleType()
    elif glue_type == 'timestamp':
        return TimestampType()
    elif glue_type == 'boolean':
        return BooleanType()
    elif glue_type.startswith('varchar'):
        return StringType()
    elif glue_type.startswith('char'):
        return StringType()
    elif glue_type == 'string':
        return StringType()
    else:
        return StringType()


# Create Spark schema using the mapping function
spark_schema = StructType([
    StructField(col['Name'], get_spark_type(col['Type'], col['Name']), True)
    for col in filtered_glue_schema
])


def generate_create_table_sql(glue_schema, table_name):
    """Generate Aurora DSQL CREATE TABLE statement from Glue schema."""
    columns = []
    filtered_schema = [col for col in glue_schema if col['Name'].lower() != 'op']

    for column in filtered_schema:
        column_name = column['Name']
        glue_type = column['Type'].lower()

        # Special handling for ID columns
        if 'id' in column_name.lower():
            if glue_type.startswith('decimal') or glue_type in ['int', 'integer', 'bigint', 'long', 'numeric']:
                dsql_type = "BIGINT"
            elif glue_type.startswith('varchar'):
                try:
                    length = glue_type.replace('varchar(', '').replace(')', '')
                    dsql_type = f"VARCHAR({length})"
                except:
                    dsql_type = "VARCHAR(30)"
            elif glue_type.startswith('char'):
                try:
                    length = glue_type.replace('char(', '').replace(')', '')
                    dsql_type = f"CHAR({length})"
                except:
                    dsql_type = "CHAR(1)"
            else:
                dsql_type = "VARCHAR(30)"
        else:
            if glue_type.startswith('decimal'):
                try:
                    parts = glue_type.replace('decimal(', '').replace(')', '').split(',')
                    precision = parts[0].strip()
                    scale = parts[1].strip() if len(parts) > 1 else '0'
                    dsql_type = f"NUMERIC({precision},{scale})"
                except:
                    dsql_type = "NUMERIC"
            elif glue_type.startswith('varchar'):
                try:
                    length = glue_type.replace('varchar(', '').replace(')', '')
                    dsql_type = f"VARCHAR({length})"
                except:
                    dsql_type = "VARCHAR(255)"
            elif glue_type.startswith('char'):
                try:
                    length = glue_type.replace('char(', '').replace(')', '')
                    dsql_type = f"CHAR({length})"
                except:
                    dsql_type = "CHAR(1)"
            else:
                base_type = glue_type.split('(')[0]
                dsql_type = data_type_mapping.get(base_type, "VARCHAR(255)")

        columns.append(f'"{column_name}" {dsql_type}')

    create_table_sql = f'CREATE TABLE public.{table_name} ({", ".join(columns)})'
    return create_table_sql


# =============================================================================
# READ AND TRANSFORM DATA
# =============================================================================

# Read raw data from S3
raw_df = spark.read.format("csv") \
    .option("header", "false") \
    .option("quote", '"') \
    .option("escape", '"') \
    .option("multiline", "true") \
    .load(s3_path)

# Get the column names from filtered schema
schema_columns = [col['Name'] for col in filtered_glue_schema]

# Rename columns to match the schema (skip _c0 which is the 'op' column)
df = raw_df.select(
    *[raw_df[f"_c{i+1}"].alias(schema_columns[i]) for i in range(len(schema_columns))]
)

# Cast the columns to their proper types according to the schema
for field in spark_schema.fields:
    df = df.withColumn(field.name, df[field.name].cast(field.dataType))


# Function to truncate string columns
def truncate_string_columns(df):
    """Truncate string columns to 30 characters."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, substring(col(field.name), 1, 30))
    return df


# Apply truncation
df = truncate_string_columns(df)

# =============================================================================
# LOAD DATA INTO AURORA DSQL
# =============================================================================

# JDBC connection properties
jdbc_url = f"jdbc:postgresql://{your_cluster_endpoint}:5432/postgres"
connection_properties = {
    "user": username,
    "password": auth_token,
    "driver": "org.postgresql.Driver",
    "ssl": "true",
    "sslmode": "require"
}

# Generate and execute CREATE TABLE statement
create_table_sql = generate_create_table_sql(glue_schema, table_name)
print("CREATE TABLE statement:")
print(create_table_sql)


# Execute DDL statements using direct JDBC connection
def execute_ddl(sql):
    """Execute DDL statements against Aurora DSQL."""
    conn = None
    stmt = None
    try:
        conn = spark.sparkContext._jvm.java.sql.DriverManager.getConnection(
            jdbc_url, username, auth_token
        )
        conn.setAutoCommit(False)
        stmt = conn.createStatement()
        stmt.execute(sql)
        conn.commit()
        print(f"Executed DDL: {sql}")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error executing DDL: {str(e)}")
        raise e
    finally:
        if stmt:
            stmt.close()
        if conn:
            conn.close()


# Execute DROP and CREATE statements
if drop_existing_table:
    execute_ddl(f"DROP TABLE IF EXISTS public.{table_name}")

execute_ddl(create_table_sql)

# Write data
try:
    total_rows = df.count()
    num_partitions = max(1, (total_rows // 9900) + 1)

    df.repartition(num_partitions).write \
        .option("isolationLevel", "REPEATABLE_READ") \
        .option("batchsize", 9900) \
        .jdbc(
            url=jdbc_url,
            table=f"public.{table_name}",
            mode="append",
            properties=connection_properties
        )

    print(f"Data insertion completed. {total_rows} rows inserted.")

    # Verify the data
    df_verify = spark.read.jdbc(
        url=jdbc_url,
        table=f"public.{table_name}",
        properties=connection_properties
    )
    print("\nSample data from target table:")
    df_verify.show(10, truncate=False)
    print(f"\nTotal rows in target table: {df_verify.count()}")

except Exception as e:
    print(f"Error: {str(e)}")
    raise e

job.commit()
