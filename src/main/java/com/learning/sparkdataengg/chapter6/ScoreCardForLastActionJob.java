package com.learning.sparkdataengg.chapter6;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Map;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_csv;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import scala.collection.JavaConverters.*;

/****************************************************************************
 * This streaming job retrieves real time Last action where duration is
 * greater than 15 seconds and maintains a redis scorecard.
 ****************************************************************************/
public class ScoreCardForLastActionJob {

    public static void main(String[] args) {

        System.out.println("******** Initiating Last Action Scorecard *************");

        //Needed for windows only
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

        Logger.getLogger("org").setLevel(Level.WARN );
        Logger.getLogger("akka").setLevel(Level.WARN);

        //Initiate  Redis
        RedisWriter.setUp();

        System.out.println("******** Starting Streaming  *************");

        try {
            //Create the Spark Session
            SparkSession spark = SparkSession
                    .builder()
                    .master("local[2]")
                    .config("spark.driver.host","127.0.0.1")
                    .config("spark.driver.bindAddress","127.0.0.1")
                    .config("spark.sql.shuffle.partitions",2)
                    .config("spark.default.parallelism",2)
                    .appName("ScoreCardLastActionProcessor")
                    .getOrCreate();

            System.out.println("Reading from Kafka..");

            //Consume the website visits topic
            Dataset<Row> rawLastActionDf = spark
                    .readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("subscribe", "spark.exercise.lastaction.long")
                    //.option("startingOffsets","earliest")
                    .load();

            Dataset<Row> lastActionDf = rawLastActionDf
                    .selectExpr("CAST(value AS STRING) as lastAction");

            lastActionDf.printSchema();
            lastActionDf
                    .writeStream()
                    .foreach(
                            new ForeachWriter<Row>() {

                                @Override public boolean open(long partitionId, long version) {
                                    return true;
                                }
                                @Override public void process(Row record) {
                                    System.out.println("Retrieved long action Record "
                                            + " : " + record.toString() );
                                }
                                @Override public void close(Throwable errorOrNull) {
                                    // Close the connection
                                }
                            }
                    )
                    .start();

            //Update country wise duration counters in real time
            lastActionDf
                    .writeStream()
                    .foreach(new RedisWriter())
                    .start();

            //Keep the process running
            final CountDownLatch latch = new CountDownLatch(1);
            latch.await();
        }
        catch(Exception e) {
            e.printStackTrace();
        }



    }
}
