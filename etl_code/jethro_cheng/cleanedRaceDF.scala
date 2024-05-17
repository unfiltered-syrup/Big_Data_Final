//spark-shell --deploy-mode client --packages com.crealytics:spark-excel_2.12:0.13.7
val filePath = "demo_2021acs5yr_nta.xlsx"

// Load the data
val df = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").option("sheetName", "DemData").load(filePath)

// Remove neighborhoods with no population
val filteredRaceDF = df.filter(col("Pop_1E") > 0)

// Select columns to use
val raceColumnsToSelect = Array("GeogName", "Borough", "Pop_2E", "Hsp1E", "WtNHE", "BlNHE", "AIANNHE", "NHPINHE", "OthNHE", "Asn1RcE", "AsnEastE", "AsnSouthE", "AsnSEastE", "AsnOAsnE")

// Selecting and renaming the columns from the dataframe
val raceColumnNames = Array("Neighborhood", "Borough", "TotalPopulation", "Hispanic", "White", "Black", "NativeAmerican", "PacificIslander", "Other", "Asian", "EastAsian", "SouthAsian", "SouthEastAsian", "OtherAsian")

val raceDF = filteredRaceDF.select(raceColumnsToSelect.map(col): _*).toDF(raceColumnNames: _*)


// Export to csv
raceDF.repartition(1).write.option("header", "true").csv("hdfs://nyu-dataproc-m/user/jhc10001_nyu_edu/FinalCode")
