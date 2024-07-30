import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# --------------------------------------------------------------EXTRACT--------------------------------------------------------------

def Read_parquet_file(path):
    data = spark.read.parquet(path)
    data = data.select('user_id', 'keyword')
    data = data.filter((data.user_id.isNotNull()) & (data.keyword.isNotNull()))
    data = data.withColumn('user_id', col('user_id').cast('int'))
    return data

def Extract_14_day(directory_path, h):
    folders = os.listdir(directory_path)
    data = 0
    for folder in folders:
        if folder[5] == h:
            folder_path = os.path.join(directory_path, folder)
            
            if data == 0:
                data = Read_parquet_file(folder_path)
            else:
                data = data.union(Read_parquet_file(folder_path))

    return data


# Find the user_ids present in both June and July to select as a sample
def Filter_userid(df6, df7):
    tmp6 = df6.groupBy('user_id').agg(count('user_id')) # Group by user_id to prevent overload during join
    tmp7 = df7.groupBy('user_id').agg(count('user_id'))
    tmp67 = tmp6.join(tmp7,on='user_id',how='inner') # Contains user_ids present in both June and July

    # Perform an inner join with df6 and df7 to filter out the user_ids that satisfy the condition and export the file for labeling
    df6 = tmp67.join(df6,on='user_id',how='inner')
    df7 = tmp67.join(df7,on='user_id',how='inner')

    # Filter the user_ids present in the top 100 user_ids. 42290 is the 100th user_id
    df6 = df6.filter(col('user_id') <= 42290)
    df7 = df7.filter(col('user_id') <= 42290)

    return df6, df7

# --------------------------------------------------------------TRANSFORM--------------------------------------------------------------

def Most_search(data, Mapping):
    data = data.groupBy('user_id', 'keyword').agg(count('keyword')).orderBy('user_id')
    data = data.join(Mapping, on='keyword', how='inner')

    data = data.groupBy('user_id', 'category').agg(sum('count(keyword)').alias('count'), concat_ws(', ', collect_list('keyword')).alias('Most_search'))

    tmp = data.groupBy('user_id').agg(max('count').alias('max_count'))
    tmp = tmp.withColumnRenamed('user_id', 'user_id_tmp')
    tmp = data.join(tmp, (data['user_id'] == tmp['user_id_tmp']) & (data['count'] == tmp['max_count']), 'inner')
    tmp = tmp.groupBy('user_id').agg(concat_ws(', ', collect_list('category')).alias('Trend'), concat_ws(', ', collect_list('Most_search')).alias('Most_search'))

    return tmp

def Transform_to_OLAP_output(df6, df7, Mapping):
    df6 = Most_search(df6, Mapping)
    df7 = Most_search(df7, Mapping)

    df6 = df6.withColumnsRenamed({'Most_search':'Most_search_T6', 'Trend':'Trend_T6'})
    df7 = df7.withColumnsRenamed({'Most_search':'Most_search_T7', 'Trend':'Trend_T7'})

    result = df6.join(df7, on='user_id', how='inner')
    result = result.withColumn('Is_changed', when(col('Trend_T6') == col('Trend_T7'), 'Unchanged')\
                    .otherwise(concat_ws(' --> ', result['Trend_T6'], result['Trend_T7'])))
    
    return result
# --------------------------------------------------------------LOAD--------------------------------------------------------------

def Load(data):
    url = 'jdbc:mysql://localhost:3306/B7_ETL'
    driver = "com.mysql.cj.jdbc.Driver"
    user = 'root'
    password = '1'
    dbtable = 'table_name_1'
    data.write.format('jdbc').options(url = url , driver = driver , dbtable = dbtable , user=user , password = password).mode('overwrite').save()

# --------------------------------------------------------------EXEC--------------------------------------------------------------

def Main():
    directory_path = '/Users/quachtuankhoi/Downloads/BigData - GEN - 7/log_search'
    category_path = '/Users/quachtuankhoi/Library/Mobile Documents/com~apple~CloudDocs/Python/Buổi 7 - Project/Mapping.json'

    df6 = Extract_14_day(directory_path, '6')
    df7 = Extract_14_day(directory_path, '7')

    Mapping = spark.read.json(category_path)
    Mapping = Mapping.select('keyword', 'category')
    Mapping = Mapping.distinct()
    Mapping = Mapping.filter(col('keyword').isNotNull() & col('category').isNotNull())

    # Filter data of the first 100 users because the Mapping table only has manually labeled keywords for the first 100 users.
    df6, df7 = Filter_userid(df6, df7)

    data = Transform_to_OLAP_output(df6, df7, Mapping)
    Load(data)

spark = SparkSession.builder.config("spark.jars.packages","com.mysql:mysql-connector-j:8.3.0").getOrCreate()
result = Main()
spark.stop() 