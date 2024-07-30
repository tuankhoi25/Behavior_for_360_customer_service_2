# Behavior_for_360_customer_service
This project is designed to analyze customers' searching behavior in the first 2 weeks of June 2022 and July 2022.

## About the data 
Users' search data is stored in PARQUET format and covers the first 2 weeks of June 2022 and July 2022.

## Procedure 
The data is processed using PySpark and then stored in a MySQL database.

### Extract data
In this process, we read data for each of the 14 days and then merge the results to obtain user interaction data for those 14 days.
```python
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
```

#### The specific function of each method

##### Extract_14_day function
This function has two parameters. The first is a directory path containing data for the first two weeks of June and July. The second is a variable named 'h' which takes the value '6' or '7' to read the data for June or July, respectively. Each file in the directory is read and processed by the Read_parquet_file function and then unioned together. The final result of this function is a DataFrame containing data from the 14 days of either June or July.
```python
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
```

##### Extract_14_day function
When called within the Extract_14_day function, this function reads the specific file and selects the attributes necessary for analysis.
```python
def Read_parquet_file(path):
    data = spark.read.parquet(path)
    data = data.select('user_id', 'keyword')
    data = data.filter((data.user_id.isNotNull()) & (data.keyword.isNotNull()))
    data = data.withColumn('user_id', col('user_id').cast('int'))
    return data
```

### Get Mapping data
Each user search query is associated with a corresponding category.
```python
Mapping = spark.read.json(category_path)
Mapping = Mapping.select('keyword', 'category')
Mapping = Mapping.distinct()
Mapping = Mapping.filter(col('keyword').isNotNull() & col('category').isNotNull())
```

### Transform data
In this process, we transform the data obtained from the Extract Data stage to determine what users searched for the most during the first 2 weeks of June and July and compare whether these search trends changed between June and July.
```python
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
```

#### The specific function of each method.

##### Most_search function
This code takes two parameters: a map table and user data from June or July. It then transforms the data as follows:

* Maps search categories for each keyword.
* Calculates the total search count for each category per user and stores this in the Most_search column.
* For each user, identify the most searched category and the corresponding search content.
```python
def Most_search(data, Mapping):
    data = data.groupBy('user_id', 'keyword').agg(count('keyword')).orderBy('user_id')
    data = data.join(Mapping, on='keyword', how='inner')

    data = data.groupBy('user_id', 'category').agg(sum('count(keyword)').alias('count'), concat_ws(', ', collect_list('keyword')).alias('Most_search'))

    tmp = data.groupBy('user_id').agg(max('count').alias('max_count'))
    tmp = tmp.withColumnRenamed('user_id', 'user_id_tmp')
    tmp = data.join(tmp, (data['user_id'] == tmp['user_id_tmp']) & (data['count'] == tmp['max_count']), 'inner')
    tmp = tmp.groupBy('user_id').agg(concat_ws(', ', collect_list('category')).alias('Trend'), concat_ws(', ', collect_list('Most_search')).alias('Most_search'))

    return tmp
```

##### Transform_to_OLAP_output function
In this code, we find the most searched content for each user in June and July using the Most_search function. Then, we combine user data from both June and July to identify their search trends. The value in the Is_changed column will be 'Unchanged' if the user's search trend remains the same in June and July. If the search trend changes, the Is_changed column will indicate the change from one category to another.
```python
def Transform_to_OLAP_output(df6, df7, Mapping):
    df6 = Most_search(df6, Mapping)
    df7 = Most_search(df7, Mapping)

    df6 = df6.withColumnsRenamed({'Most_search':'Most_search_T6', 'Trend':'Trend_T6'})
    df7 = df7.withColumnsRenamed({'Most_search':'Most_search_T7', 'Trend':'Trend_T7'})

    result = df6.join(df7, on='user_id', how='inner')
    result = result.withColumn('Is_changed', when(col('Trend_T6') == col('Trend_T7'), 'Unchanged')\
                    .otherwise(concat_ws(' --> ', result['Trend_T6'], result['Trend_T7'])))
    
    return result
```

### Load data
Store the OLAP output just obtained into the MySQL database.
```python
def Load(data):
    url = 'jdbc:mysql://localhost:3306/B7_ETL'
    driver = "com.mysql.cj.jdbc.Driver"
    user = 'root'
    password = '1'
    dbtable = 'table_name_1'
    data.write.format('jdbc').options(url = url , driver = driver , dbtable = dbtable , user=user , password = password).mode('overwrite').save()
```

### Final data
|user_id|            Trend_T6|      Most_search_T6|        Trend_T7|      Most_search_T7|          Is_changed|
|-------|--------------------|--------------------|----------------|--------------------|--------------------|
|   1396|             K-drama|yêu trong đau thu...|         C-drama|phim tươi cười ph...| K-drama --> C-drama|
|   1804|             C-drama|    nửa là đường mật|         C-drama|thế giới này khôn...|           Unchanged|
|   1969|               Anime|attack on titan (...|         Another|               GIO P|   Anime --> Another|
|   2032|    K-drama, Cartoon|love in sadness, ...|           Comic|      THIEN THAN BON|K-drama, Cartoon ...|
|   2325|   C-drama, C-people|nhất sinh nhất th...|         C-drama|        tinh hán xán|C-drama, C-people...|
|   2384|             C-drama|       ngoc lau xuan|         K-drama|    vẻ đẹp đích thực| C-drama --> K-drama|
|   2816|             C-drama|định mệnh  anh ph...|         C-drama|     vân tịch truyện|           Unchanged|
|   3266|             E-drama|Liên Minh Công Lý...|        VN-drama|loi nho vao d, lố...|E-drama --> VN-drama|
|   3361|Anime, E-drama, T...|tây hành kỷ, vuot...|         C-drama|         lộc đỉnh ký|Anime, E-drama, T...|
|   3371|                News|                 vtv|  Football, News|hoang anh gia lai...|News --> Football...|
|   3691|             E-drama|công viên kỷ jura...|           Comic|tiếng gọi nơi hoa...|   E-drama --> Comic|
|   3926|               Anime|fairy ta, người y...|           Anime|học viện anh hùng...|           Unchanged|
|   4007|             Cartoon| tom and jerry tales|         Another|lật bật rap và, b...| Cartoon --> Another|
|   4833|             C-drama|chúng ta không bi...|Another, C-drama|nghìn, nghìn lẻ m...|C-drama --> Anoth...|
|   4857|             C-drama|     hồ sơ trinh sát|         C-drama|     pháp y tầm minh|           Unchanged|
|   4908|               Anime|     conan, doraemon|           Anime|               conan|           Unchanged|
|   5187|            K-people|          Kim Tae Ri|         K-drama|high school king ...|K-people --> K-drama|
|   5748|             T-drama|lời nguyền ma lai...|           Anime|chuyen sinh thanh...|   T-drama --> Anime|
|   5968|            C-people|   Địch Lệ Nhiệt Ba |         C-drama|lương sinh, liệu ...|C-people --> C-drama|
|   6112|            Football|u23 thái lan, u23...|        Football|xem lai u19, xem ...|           Unchanged|

### Results Achieved from the Data ETL Process
* For categories with consistent trends over time, we can consider investing more in those categories. For example, the K-drama and C-drama categories typically have a large and stable audience, so we might consider acquiring the rights to broadcast films in those categories.
* Recommend movies based on popular films that are similar to what the user is currently watching. For example, if a specific user is watching movie A and most people who watched movie A also tend to watch movie B, we would recommend movie B to that user.

### Areas for Improvement
* The database needs to be normalized to 2NF (and higher if necessary) due to multivalued attributes.
* A more comprehensive and detailed mapping table is needed to label each keyword that users search for.