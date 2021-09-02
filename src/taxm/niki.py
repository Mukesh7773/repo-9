from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

class Taxmdata:
    def data(self,client_code,tenantid,container_name,account_name,):
        file_name= client_code +'_'+tenantid+'.csv'

        adls_path = 'abfss://%s@%s.dfs.core.windows.net/%s' % (container_name, account_name)
        df_pyspark = spark.read.csv(adls_path + file_name, header=True, inferSchema=True)
        return df_pyspark
        exclude = df_pyspark.select("exclude")
        exclude_array = [str(row.exclude) for row in exclude.collect()]
        new_dictionary = {};
        for x in exclude_array:
            if x == 'true':
                new_dictionary[x] = "100% Compliant"
        else:
            new_dictionary[x] = "0% Compliant"
        compliancy_csv = list(map(list, new_dictionary.items()))
        new_df_pyspark = spark.createDataFrame(compliancy_csv, ["datasource_name", "Compliance Result"]).show()
        return new_df_pyspark