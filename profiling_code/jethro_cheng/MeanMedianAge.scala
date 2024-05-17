//spark-shell --deploy-mode client --packages com.crealytics:spark-excel_2.12:0.13.7
val filePath = "demo_2021acs5yr_nta.xlsx"

// Load the data 
val df = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").option("sheetName", "DemData").load(filePath)

// Remove neighborhoods with no population
val filteredAgeDF = df.filter(col("Pop_1E") > 0)

// Define the columns to be selected based on their indices
val AgeColumnIndices = Array(3, 5, 6, 21, 26, 31, 36, 41, 46, 51, 56, 61, 66, 71, 76, 81, 86, 91, 96, 101, 106)

val populationColumns = Array("GeogName", "Borough", "Pop_1E", "PopU5E", "Pop5t9E", "Pop10t14E", "Pop15t19E", "Pop20t24E", "Pop25t29E", "Pop30t34E", "Pop35t39E", "Pop40t44E", "Pop45t49E", "Pop50t54E", "Pop55t59E", "Pop60t64E", "Pop65t69E", "Pop70t74E", "Pop75t79E", "Pop80t84E", "Pop85plE")

// Names for the new DataFrame columns
val ageGroupNames = Array("Neighborhood", "Borough", "TotalPopulation","<5", "5-9", "10-14", "15-19", "20-24", "25-29", "30-34", "35-39", "40-44", "45-49", "50-4", "55-59", "60-64", "65-69", "70-74", "75-79", "80-84", ">85")

val ageDF = filteredAgeDF.select(populationColumns.map(col): _*).toDF(ageGroupNames: _*)



// Code for Mean Age
// Calculate the weighted mean age assuming you know the midpoint of each age range
val ageMidpoints = Array(3, 7, 12, 17, 22, 27, 32, 37, 42, 47, 52, 57, 62, 67, 72, 77, 82, 87)

val weightedSumColumn = ageMidpoints.zipWithIndex.map { case (midpoint, index) =>
  col(ageGroupNames(index + 2)) * lit(midpoint)
}.reduce(_ + _)

val totalPopulationColumn = col("TotalPopulation")

val meanAgeColumn = (weightedSumColumn / totalPopulationColumn).alias("MeanAge")
val meanAgeDF = ageDF.withColumn("MeanAge", meanAgeColumn)

// Show results
meanAgeDF.show()






// Code for Median Age
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/// Create a data frame for median calculation
val expandedAgeDF = meanAgeDF.select(
  col("Neighborhood"), 
  col("Borough"), 
  col("TotalPopulation"),
  posexplode(array(
    ageGroupNames.drop(3).zip(ageMidpoints).map {
      case (ageCol, midpoint) => struct(lit(midpoint).alias("Midpoint"), col(ageCol).alias("Population"))
    }: _*
  ))
).select(
  col("Neighborhood"), 
  col("Borough"), 
  col("TotalPopulation"),
  col("col.Midpoint"), 
  col("pos"), 
  col("col.Population")
)

expandedAgeDF.createOrReplaceTempView("expanded_ages")


// Create a SQL data frame
val cumulativeDF = spark.sql("""
  SELECT Neighborhood, Borough, TotalPopulation, Midpoint, Population,
         sum(Population) OVER (PARTITION BY Neighborhood, Borough ORDER BY Midpoint) AS CumulativePopulation
  FROM expanded_ages
""")

cumulativeDF.createOrReplaceTempView("cumulativeDF")

// Calculate Median Age using SQL
val medianAgeDF = spark.sql("""
  SELECT Neighborhood, Borough, Midpoint AS MedianAge
  FROM (
    SELECT *, 
           row_number() OVER (PARTITION BY Neighborhood, Borough ORDER BY Midpoint) as row_num
    FROM cumulativeDF
    WHERE CumulativePopulation >= TotalPopulation / 2
  ) AS sub
  WHERE sub.row_num = 1
""")

// Show results
medianAgeDF.show()


// Join the DataFrames to combine Mean Age and Median Age
val meanMedianAgeDF = meanAgeDF.join(medianAgeDF, Seq("Neighborhood", "Borough"), "inner").select(
    meanAgeDF("Neighborhood"),
    meanAgeDF("Borough"),
    meanAgeDF("MeanAge"),
    medianAgeDF("MedianAge")
  )

// Show the combined results
meanMedianAgeDF.show()

// Export to csv
meanMedianAgeDF.repartition(1).write.option("header", "true").csv("hdfs://nyu-dataproc-m/user/jhc10001_nyu_edu/FinalCode")