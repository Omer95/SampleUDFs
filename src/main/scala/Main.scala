import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello world!")
    val spark = SparkSession.builder.master("local").appName("Sample UDFs").getOrCreate()
    val incrementItem = (item: Int) => item + 1
    spark.udf.register("incrementItem", incrementItem)
    spark.sql("SELECT incrementItem(5) AS increment_item").show()

    val data = Seq((1001, "omer"), (1002, "ali"))
    val columns = Seq("pid", "name")
    val df = spark.createDataFrame(data).toDF(columns: _*)
    df.show()

    val newData = Seq(Row(1001, "omer", Seq(1,0,1)), Row(1002, "ali", Seq(0,0,1)), Row(1003, "david", Seq(1,1,1)))
    val schema = StructType(Seq(
      StructField("pid", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("variants", ArrayType(IntegerType), nullable = true)
    ))
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(newData), schema)
    df2.show()

    val incrementList = (list: Seq[Int]) => { list.map(_ + 1) }
    val incrementListFunc = udf(incrementList)

    df2.withColumn("incremented", incrementListFunc(col("variants"))).show()

    df2.createOrReplaceGlobalTempView("geneticVariants")

    spark.udf.register("incrementList", udf(incrementList))

  }
}