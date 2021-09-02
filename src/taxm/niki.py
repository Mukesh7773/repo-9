from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

class Taxmdata:
    def data(self,client_code,tenantid,container_name,account_name,relative_path):
        file_name= client_code +'_'+tenantid+'.csv'

        adls_path = 'abfss://%s@%s.dfs.core.windows.net/%s' % (container_name, account_name,relative_path)
        df_pyspark = spark.read.csv(adls_path + file_name, header=True, inferSchema=True)
        return df_pyspark


