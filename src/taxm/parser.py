
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


class Parser:
    def parserdata(self,df_pyspark):
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