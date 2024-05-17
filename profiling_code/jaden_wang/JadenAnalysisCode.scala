val demo_data = "jaden_code_drop/demo_data_cleaned.csv" //data cleaned by Jethro
val complaints_data = "jaden_code_drop/complaints_data_cleaned.csv" //data cleaned by Jaden
//read complaints and demographic data
val demo_df = spark.read.option("header", "true").csv(demo_data)
val demo_df_clean = demo_df.withColumn("Neighborhood", upper(regexp_replace(col("Neighborhood"), "[()]", "")))
val complaints_df = spark.read.option("header", "true").csv(complaints_data)
val complaints_df_clean =  complaints_df.withColumn("NTA", regexp_replace(col("NTA"), "[()]", ""))

//join the two dataframes
val joined_df = demo_df_clean.join(complaints_df_clean, demo_df_clean("Neighborhood") === complaints_df_clean("NTA"), "inner")

//create complaints per capita features
val df_additional_features = joined_df.withColumn("complaints_per_capita", col("total_complaints") / col("TotalPopulation"))
.withColumn("indoor_air_quality_complaints_per_capita", col("indoor_air_quality_complaints") / col("TotalPopulation"))
.withColumn("asbestos_complaints_per_capita", col("asbestos_complaints") / col("TotalPopulation"))
.withColumn("mold_complaints_per_capita", col("mold_complaints") / col("TotalPopulation"))
.withColumn("sewage_complaints_per_capita", col("sewage_complaints") / col("TotalPopulation"))

//normalize the data from 0 to 1
val minMaxDF = df_additional_features.agg(
  min("complaints_per_capita").alias("min_cp"),
  max("complaints_per_capita").alias("max_cp")
)
//get the min and max values
val minMax = minMaxDF.collect().map(row => (row.getDouble(0), row.getDouble(1))).head
val minCp = minMax._1
val maxCp = minMax._2

val scaledDF = df_additional_features.withColumn(
  "complaints_per_capita_scaled",
  (col("complaints_per_capita") - lit(minCp)) / (lit(maxCp) - lit(minCp))
)

//write the output
scaledDF.repartition(1).write.option("header", "true").csv("jaden_code_drop/analysis_output/")
