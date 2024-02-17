import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object data_etl {



      def main(args: Array[String]): Unit = {

      val spark = SparkSession.builder()
        .appName("SparkSalesAnalysis")
        .master("local[*]")
        .getOrCreate()


      try {

        val salesDF = readData(spark, "src/main/resources/sales_data.csv", salesSchema)
        val productDF = readData(spark, "src/main/resources/product_data.csv", productSchema)

        val joinedDF = salesDF.join(productDF, Seq("Product_ID"), "inner")

        val productSalesDF = joinedDF
          .withColumn("Total_Sales_Amount", col("Quantity") * col("Price"))
          .groupBy("Product_ID", "Product_Name", "Category", "Unit_Price")
          .agg(sum("Total_Sales_Amount").alias("Total_Sales_Amount"))

        val categorySalesDF = productSalesDF
          .groupBy("Category")
          .agg(sum("Total_Sales_Amount").alias("Total_Sales_Amount"))

        productDF.show()
        categorySalesDF.show()

        productSalesDF.write.csv("src/main/output/product_sales")
        categorySalesDF.write.csv("src/main/output/category_sales")

      } catch {
        case ex: Exception =>
          println(s"An error occurred: ${ex.getMessage}")
      } finally {
        spark.stop()
      }
    }

    val salesSchema = StructType(Seq(
      StructField("Transaction_ID", StringType, nullable = false),
      StructField("Product_ID", StringType, nullable = false),
      StructField("Quantity", IntegerType, nullable = false),
      StructField("Price", DoubleType, nullable = false)
    ))

    val productSchema = StructType(Seq(
      StructField("Product_ID", StringType, nullable = false),
      StructField("Product_Name", StringType, nullable = false),
      StructField("Category", StringType, nullable = false),
      StructField("Unit_Price", DoubleType, nullable = false)
    ))

    def readData(spark: SparkSession, filePath: String, schema: StructType): DataFrame = {
      val df = spark.read
        .option("header", "true")
        .schema(schema)
        .csv(filePath)

      val columnsWithNull = schema.filter(field => df.filter(col(field.name).isNull).count() > 0).map(_.name)
      if (columnsWithNull.nonEmpty) {
        throw new IllegalArgumentException(s"Null values found in columns: ${columnsWithNull.mkString(", ")}")
      }

      schema.fields.foreach { field =>
        if (df.schema(field.name).dataType != field.dataType) {
          throw new IllegalArgumentException(s"Invalid data type for column '${field.name}'. Expected: ${field.dataType}, Actual: ${df.schema(field.name).dataType}")
        }
      }

      df
    }


}
