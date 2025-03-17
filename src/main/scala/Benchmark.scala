import java.io.PrintWriter
      import org.knowm.xchart.{XYChart, XYChartBuilder, BitmapEncoder}
      import org.knowm.xchart.style.Styler
      import org.apache.spark.sql.{DataFrame, Row, SparkSession}
      import org.apache.spark.sql.functions._
      import org.apache.spark.sql.types._
      import scala.util.Random

      object Benchmark {

        def createSparkSession(): SparkSession = {
          val spark = SparkSession.builder
            .appName("Benchmark")
            .master("local[*]")
            .config("spark.driver.memory", "30g")
            .config("spark.ui.enabled", "true")
            .config("spark.ui.port", "4040")
            .getOrCreate()
          spark
        }

        def generateBenchmarkData(mapNumKeys: Int, numRecords: Int, spark: SparkSession): (DataFrame, DataFrame) = {
          val records = (1 to numRecords).map { _ =>
            val mapData = (0 until mapNumKeys).map { i =>
              if (i < mapNumKeys * 0.25) s"key_$i" -> Random.nextInt(100).toString
              else if (i < mapNumKeys * 0.5) s"key_$i" -> Random.nextDouble().toString
              else if (i < mapNumKeys * 0.75) s"key_$i" -> java.util.UUID.randomUUID().toString
              else s"key_$i" -> Random.nextBoolean().toString
            }.toMap
            Row(mapData)
          }

          val schema = StructType(Array(StructField("data", MapType(StringType, StringType), true)))
          val df = spark.createDataFrame(spark.sparkContext.parallelize(records), schema).repartition(10)
          val dfVariant = df.withColumn("data", parse_json(to_json(col("data"))))

          (df, dfVariant)
        }

        def benchmarkMapAccess(df: DataFrame, lookupCount: Int): Double = {
          val times = (1 to 3).map { i =>
            println(s"Running MapType benchmark iteration $i with $lookupCount keys")
            val start = System.currentTimeMillis()
            val selected = (0 until lookupCount).map { i =>
              if (i < lookupCount * 0.25) col(s"data.key_$i").cast(IntegerType).alias(s"key_$i")
              else if (i < lookupCount * 0.5) col(s"data.key_$i").cast(DoubleType).alias(s"key_$i")
              else if (i < lookupCount * 0.75) col(s"data.key_$i").cast(StringType).alias(s"key_$i")
              else col(s"data.key_$i").cast(BooleanType).alias(s"key_$i")
            }
            df.select(selected: _*).write.format("noop").mode("overwrite").save()
            val time = (System.currentTimeMillis() - start) / 1000.0
            println(s"MapType - $lookupCount lookups - benchmark iteration $i completed in $time seconds")
            time
          }
          times.sorted.apply(1) // Return the median time
        }


        def benchmarkVariantAccess(df: DataFrame, lookupCount: Int): Double = {
          val times = (1 to 3).map { i =>
            println(s"Running VariantType benchmark iteration $i with $lookupCount keys")
            val start = System.currentTimeMillis()
            val selected = (0 until lookupCount).map { i =>
              if (i < lookupCount * 0.25)
                variant_get(col("data"), s"$$.key_$i", "int").alias(s"key_$i")
              else if (i < lookupCount * 0.5)
                variant_get(col("data"), s"$$.key_$i", "double").alias(s"key_$i")
              else if (i < lookupCount * 0.75)
                variant_get(col("data"), s"$$.key_$i", "string").alias(s"key_$i")
              else
                variant_get(col("data"), s"$$.key_$i", "boolean").alias(s"key_$i")
            }
            df.select(selected: _*).write.format("noop").mode("overwrite").save()
            val time = (System.currentTimeMillis() - start) / 1000.0
            println(s"VariantType - $lookupCount lookups - benchmark iteration $i completed in $time seconds")
            time
          }
          times.sorted.apply(1) // Return the median time
        }

        def warmUp(spark: SparkSession, numRecords: Int): Unit = {
          val numKeys = 10
          val (dfMap, dfVariant) = generateBenchmarkData(numKeys, numRecords, spark)
          benchmarkMapAccess(dfMap, numKeys)
          benchmarkVariantAccess(dfVariant, numKeys)
        }

        def main(args: Array[String]): Unit = {
          val spark = createSparkSession()
          val numRecords = 1000
          warmUp(spark, numRecords)

          val benchmarkResults = (0 to 10000 by 1000).map { numKeys =>
            println(s"Running benchmark with $numKeys keys")
            val (dfMap, dfVariant) = generateBenchmarkData(numKeys, numRecords, spark)
            val mapTime = benchmarkMapAccess(dfMap, numKeys)
            val variantTime = benchmarkVariantAccess(dfVariant, numKeys)
            (numKeys, mapTime, variantTime)
          }

          val writer = new PrintWriter("benchmark_results_write.csv")
          writer.println("numKeys,mapTime,variantTime")
          benchmarkResults.foreach { case (numKeys, mapTime, variantTime) =>
            writer.println(s"$numKeys,$mapTime,$variantTime")
          }
          writer.close()

        val chart = new XYChartBuilder().width(800).height(600).title(s"Performance Comparison: MapType vs VariantType n=$numRecords")
            .xAxisTitle("Number of elements").yAxisTitle("Time (seconds)").build()

          chart.getStyler.setChartTitleVisible(true)
          chart.getStyler.setLegendPosition(Styler.LegendPosition.InsideNW)
          chart.getStyler.setMarkerSize(5)

          val numKeys = benchmarkResults.map(_._1.toDouble).toArray
          val mapTimes = benchmarkResults.map(_._2.toDouble).toArray
          val variantTimes = benchmarkResults.map(_._3.toDouble).toArray

          chart.addSeries("MapType Time", numKeys, mapTimes)
          chart.addSeries("VariantType Time", numKeys, variantTimes)

          BitmapEncoder.saveBitmap(chart, "benchmark_performance_comparison", BitmapEncoder.BitmapFormat.PNG)

          spark.stop()
        }
      }