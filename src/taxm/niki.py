from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

class Taxmdata:
    def data(**kwargs):
        client_code= kwargs['client_id']
        tenantid = kwargs['tenantid']
        container_name =kwargs['container_name']
        account_name =kwargs['account_name']
        relative_path =kwargs['relative_path']


        # clientid = 12321
        file_name= client_code +'_'+tenantid+'.csv'

        adls_path = 'abfss://%s@%s.dfs.core.windows.net/%s' % (container_name, account_name,relative_path)
        df_pyspark = spark.read.csv(adls_path + file_name, header=True, inferSchema=True)
        df_pyspark.show()
        return df_pyspark



