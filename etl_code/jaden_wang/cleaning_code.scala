import org.apache.spark.sql.functions.upper

val path = "jaden_code_drop/complaints_data_ingested.csv"
val df = spark.read.option("header", "false").csv(path)

//assign colum names
val new_column_names = Array("id", "street_number", "street_name", "address", "zip", "borough",
"complaint_type", "complaint_descriptor", "date_received", "latitude",
"longitude", "community_board", "council_district", "census_tract" , "BIN", "BBL", "NTA")


val df_with_col_names = df.toDF(new_column_names:_*)

//list of columns to be converted to uppercase
val tagetcolumns = Seq("street_name", "address", "borough", "complaint_type", "complaint_descriptor", "NTA")

val df_upper = tagetcolumns.foldLeft(df_with_col_names) { (df, colname) =>
  df.withColumn(colname, upper(col(colname)))
}

val df_date = df_upper.select(
col("id"), //select id
//split date and time and save in separate columns
split(split(col("date_received"), " ").getItem(0), "/").getItem(0).as("month"), 
split(split(col("date_received"), " ").getItem(0), "/").getItem(1).as("day"), 
split(split(col("date_received"), " ").getItem(0), "/").getItem(2).as("year"), 
split(col("date_received"), " ").getItem(1).as("time"))
//drop original date column
.drop("date_received")

//innerjoin the original df with the newly generated columns
val innerJoin = df_upper.join(df_date, Seq("id"))


val df_grouped = innerJoin.groupBy("NTA")
.agg(
    count("*").alias("total_complaints"),
    sum(when(col("complaint_type")==="INDOOR AIR QUALITY", 1).otherwise(0)).alias("indoor_air_quality_complaints"),
    sum(when(col("complaint_type")==="ASBESTOS", 1).otherwise(0)).alias("asbestos_complaints"),
    sum(when(col("complaint_type")==="MOLD", 1).otherwise(0)).alias("mold_complaints"),
    sum(when(col("complaint_type")==="INDOOR SEWAGE", 1).otherwise(0)).alias("sewage_complaints"),
)

df_grouped.repartition(1).write.option("header", "true").csv("jaden_code_drop/cleaning_output")


