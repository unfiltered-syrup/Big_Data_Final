//spark-shell --deploy-mode client --packages com.crealytics:spark-excel_2.12:0.13.7
val filePath = "demo_2021acs5yr_nta.xlsx"

// Load the data
val df = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").option("sheetName", "DemData").load(filePath)

// Remove neighborhoods with no population
val filteredAgeDF = df.filter(col("Pop_1E") > 0)

// Define the columns to be selected based on their indices
val AgeColumnIndices = Array(3, 5, 6, 21, 26, 31, 36, 41, 46, 51, 56, 61, 66, 71, 76, 81, 86, 91, 96, 101, 106)

// Select columns to use
val populationColumns = Array("GeogName", "Borough", "Pop_1E", "PopU5E", "Pop5t9E", "Pop10t14E", "Pop15t19E", "Pop20t24E", "Pop25t29E", "Pop30t34E", "Pop35t39E", "Pop40t44E", "Pop45t49E", "Pop50t54E", "Pop55t59E", "Pop60t64E", "Pop65t69E", "Pop70t74E", "Pop75t79E", "Pop80t84E", "Pop85plE")

// Selecting and renaming the columns from the dataframe
val ageGroupNames = Array("Neighborhood", "Borough", "TotalPopulation","<5", "5-9", "10-14", "15-19", "20-24", "25-29", "30-34", "35-39", "40-44", "45-49", "50-4", "55-59", "60-64", "65-69", "70-74", "75-79", "80-84", ">85")

val ageDF = filteredAgeDF.select(populationColumns.map(col): _*).toDF(ageGroupNames: _*)

// Export to csv
ageDF.repartition(1).write.option("header", "true").csv("hdfs://nyu-dataproc-m/user/jhc10001_nyu_edu/FinalCode")