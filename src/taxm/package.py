
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


class Pyspark:
    @staticmethod
    def pyspark_parser(self,file_name, account_name, container_name, relative_path):
        # account_name = "parserdatalake"
        # container_name = "parserfs"
        # relative_path = ""
        adls_path = 'abfss://%s@%s.dfs.core.windows.net/%s' % (container_name, account_name, relative_path)
        df_pyspark = spark.read.csv(adls_path + file_name, header=True, inferSchema=True)
        df_pyspark.show()
        exclude = df_pyspark.select("exclude")
        exclude_array = [str(row.exclude) for row in exclude.collect()]
        new_dictionary = {};
        for x in exclude_array:
            if x == 'true':
                new_dictionary[x] = "100% Compliant"
        else:
            new_dictionary[x] = "0% Compliant"
        compliancy_csv = list(map(list, new_dictionary.items()))
        new_df_pyspark = spark.createDataFrame(compliancy_csv, ["Name", "Compliance Result"]).show()
        return new_df_pyspark


    def person(self ,name ):
        self.name=name