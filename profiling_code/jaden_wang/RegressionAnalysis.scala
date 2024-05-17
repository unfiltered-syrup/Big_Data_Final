import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.ml.regression.LinearRegression
import java.io.{File, PrintWriter}


//read de data 
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("jaden_code_drop/dataset_additional_features.csv")

//make the column IsMajorityWhite a binary column
val df_with_binary = df.withColumn("IsMajorityWhite", when(col("IsMajorityWhite") === "TRUE", 1).otherwise(0))

//create the pipeline
//indexer to convert the Borough column to a numerical column
val indexer = new StringIndexer()
  .setInputCol("Borough")
  .setOutputCol("BoroughIndex")
//assembler to create the features column
val assembler = new VectorAssembler()
  .setInputCols(Array("TotalPopulation", "Hispanic", "White", "Black", "NativeAmerican", 
    "PacificIslander", "Other", "Asian", "IsMajorityWhite"))
  .setOutputCol("features")
//linear regression model
val lr = new LinearRegression()
  .setLabelCol("complaints_per_capita_scaled")
  .setFeaturesCol("features")
  .setMaxIter(10)
  .setRegParam(0.01)
  .setElasticNetParam(0.1)
//pipeline initialized
val pipeline = new Pipeline()
  .setStages(Array(indexer, assembler, lr))
//fit the model
val model = pipeline.fit(df_with_binary)
//get the coefficients and record in txt file
val modelLinear = model.stages.last.asInstanceOf[LinearRegressionModel]
val coefficients = modelLinear.coefficients.toArray
val featuresArray = Array("TotalPopulation", "Hispanic", "White", "Black", "NativeAmerican", 
    "PacificIslander", "Other", "Asian", "IsMajorityWhite")

featuresArray.zip(coefficients).foreach { case (feature, coeff) =>
  println(s"$feature: $coeff")
}

val writer = new PrintWriter(new File("model_coefficients.txt"))
writer.println("Model Coefficients (Linear Regression)")
writer.println("======================================")
try {
  featuresArray.zip(coefficients).foreach { case (feature, coeff) =>
  writer.println(s"$feature: $coeff")
  }
} finally {
  writer.println("======================================")
  writer.close()
}