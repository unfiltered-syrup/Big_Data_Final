//spark-shell --deploy-mode client --packages com.crealytics:spark-excel_2.12:0.13.7
val filePath = "demo_2021acs5yr_nta.xlsx"

// Load the data
val df = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").option("sheetName", "DemData").load(filePath)

// Remove neighborhoods with no population
val filteredRaceDF = df.filter(col("Pop_1E") > 0)

// Assume column names are obtained and confirmed from schema, then proceed with these names
val raceColumnsToSelect = Array("GeogName", "Borough", "Pop_2E", "Hsp1E", "WtNHE", "BlNHE", "AIANNHE", "NHPINHE", "OthNHE", "Asn1RcE", "AsnEastE", "AsnSouthE", "AsnSEastE", "AsnOAsnE")

// Selecting and renaming the columns from the dataframe
val raceColumnNames = Array("Neighborhood", "Borough", "TotalPopulation", "Hispanic", "White", "Black", "NativeAmerican", "PacificIslander", "Other", "Asian", "EastAsian", "SouthAsian", "SouthEastAsian", "OtherAsian")

val raceDF = filteredRaceDF.select(raceColumnsToSelect.map(col): _*).toDF(raceColumnNames: _*)





// Majority Race 
// Calculate the percentage for each race column relative to the total population
val raceColumns = Array("Hispanic", "White", "Black", "NativeAmerican", "PacificIslander", "Other", "Asian", "EastAsian", "SouthAsian", "SouthEastAsian", "OtherAsian")

// Apply percentage calculation to each race column
val percentagesDF = raceColumns.foldLeft(raceDF) { (currentDF, column) =>
  currentDF.withColumn(column, col(column).cast("double") / col("TotalPopulation").cast("double") * 100)
}

// Add a column for the greatest value among race percentages
val greatestValueDF = percentagesDF.withColumn("GreatestValue", greatest(raceColumns.map(col): _*))

// Define a column to store the name of the majority race
val newDF = greatestValueDF.withColumn(
  "MajorityRace",
  when(col("Hispanic") === col("GreatestValue"), lit("Hispanic"))
    .when(col("White") === col("GreatestValue"), lit("White"))
    .when(col("Black") === col("GreatestValue"), lit("Black"))
    .when(col("NativeAmerican") === col("GreatestValue"), lit("NativeAmerican"))
    .when(col("PacificIslander") === col("GreatestValue"), lit("PacificIslander"))
    .when(col("Other") === col("GreatestValue"), lit("Other"))
    .when(col("Asian") === col("GreatestValue"), lit("Asian"))
    .when(col("EastAsian") === col("GreatestValue"), lit("EastAsian"))
    .when(col("SouthAsian") === col("GreatestValue"), lit("SouthAsian"))
    .when(col("SouthEastAsian") === col("GreatestValue"), lit("SouthEastAsian"))
    .when(col("OtherAsian") === col("GreatestValue"), lit("OtherAsian"))
    .otherwise(lit("Undefined"))
)

// Add a boolean column that is true if the MajorityRace is "White" and false otherwise
val majorityRaceDF = newDF.withColumn("IsMajorityWhite", col("MajorityRace") === lit("White"))

// Show the DataFrame
majorityRaceDF.show()

// Export to csv
majorityRaceDF.repartition(1).write.option("header", "true").csv("hdfs://nyu-dataproc-m/user/jhc10001_nyu_edu/FinalCode")