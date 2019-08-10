import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.sql.Date
import SparkFirst.crimesInfoDataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DateType, IntegerType}

object RecipeDetail extends App{

  val spark = SparkSession.builder().appName("Process Recipe Details").master("local[*]").getOrCreate();
  val recipeJsonData = spark.read.json("C:\\Users\\KathirNanjundan\\IdeaProjects\\Example\\file\\recipes.json");

  val recipeDetailWithId = recipeJsonData.withColumn("recipe_id",monotonically_increasing_id());
  val totalPrepTimeDF = processTotalTime(recipeDetailWithId,"prepTime","prepHour","prepMinutes","prepTotalMinutes");
  val totalCookTimeDF = processTotalTime(totalPrepTimeDF,"cookTime","cookHour","cookMinutes","cookTotalMinutes");

  val totalTimeRecipeDF = totalCookTimeDF.withColumn("totalTimeToFinish", col("cookTotalMinutes").plus(col("prepTotalMinutes")));
  val updatedStatusDF = totalTimeRecipeDF.withColumn(
    "finalStatus",when(col("totalTimeToFinish").geq(60),"HARD")
      .when(col("totalTimeToFinish").leq(60).and(col("totalTimeToFinish").geq(30)),"Medium")
      .when(col("totalTimeToFinish").leq(30),"Easy")
      .otherwise("UnKnown"))

  val countBasedStatusDF = updatedStatusDF.groupBy(col("finalStatus")).agg(count("finalStatus"));

  countBasedStatusDF.show()

  def processTotalTime(recipeDetailWithId: DataFrame, timeColumn: String, prepHour: String, prepMinutes: String, totalInMinutes : String): DataFrame ={
    val recipeRemovalPTDF = recipeDetailWithId.withColumn(timeColumn, regexp_replace(col(timeColumn),"PT",""))
    val extractHourFromTimeDF = recipeRemovalPTDF.withColumn(prepHour,when(col(timeColumn).contains("H"),substring_index(col(timeColumn),"H",1))
      .otherwise(0))
    val extractHourFromMinutesDF = extractHourFromTimeDF.withColumn(prepMinutes,when(col(timeColumn).contains("M"),substring(col(timeColumn),-3,8))
      .otherwise(0));
    val hourConvToMinutesDF = extractHourFromMinutesDF.withColumn(prepHour,col(prepHour).multiply(60));
    val rmMinutesDF = hourConvToMinutesDF.withColumn(prepMinutes, regexp_replace(col(prepMinutes),"M",""))
    val prepTimeFinalDF = rmMinutesDF.withColumn(totalInMinutes, col(prepHour).plus(col(prepMinutes)))
    return prepTimeFinalDF;
  }
}
