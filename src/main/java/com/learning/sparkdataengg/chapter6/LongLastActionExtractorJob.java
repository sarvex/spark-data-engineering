package com.learning.sparkdataengg.chapter6;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

public class LongLastActionExtractorJob {

    public static void main(String[] args) {

        System.out.println("Starting Long Last Action Extraction Job");

        //Needed for windows only. Use hadoop 3
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

        //Set logger levels so we are not going to get info messages from underlying libraries
        Logger.getLogger("org").setLevel(Level.WARN );
        Logger.getLogger("akka").setLevel(Level.WARN);

        //Upload for each country
        //Set the dates correctly - as the original data was generated based on the
        //system date on which the WebsiteVisitsDataGenerator was run
        extractLongAction("2021-07-20", "2021-07-21");
    }

    private static void extractLongAction(String startDate, String endDate) {

        try {

            System.out.println("Running long last action extraction " +
                    "for period " + startDate + " to " + endDate);

            //Run a query to find the min and max IDs for the data to process
            //This is run in the driver program.
            String boundsQuery=
                    "SELECT min(ID) as MIN_ID ,max(ID) as MAX_ID " +
                            "FROM visit_stats " +
                            "WHERE INTERVAL_TIMESTAMP BETWEEN " +
                            "'" +  startDate + "' AND '" + endDate + "' " ;

            //Setup DB connection
            Class.forName("org.mariadb.jdbc.Driver");
            //Connect to MariaDB
            Connection warehouseConn = DriverManager.getConnection(
                    "jdbc:mariadb://localhost:3306/website_stats",
                    "spark",
                    "spark");

            ResultSet rsBounds =
                    warehouseConn
                            .createStatement()
                            .executeQuery(boundsQuery);
            int minBounds=0;
            int maxBounds=0;
            while(rsBounds.next()) {
                minBounds=rsBounds.getInt(1);
                maxBounds=rsBounds.getInt(2);
            }

            System.out.println("Bounds for the Query are "
                    + minBounds + " and " + maxBounds);


            //Create the Spark Session
            SparkSession spark = SparkSession
                    .builder()
                    .master("local[2]")
                    .config("spark.driver.host","127.0.0.1")
                    .config("spark.driver.bindAddress","127.0.0.1")
                    .config("spark.sql.shuffle.partitions", 2)
                    .config("spark.default.parallelism", 2)
                    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", true)
                    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version","2")
                    .appName("LongLastActionExtractionJob")
                    .getOrCreate();

            spark.sparkContext().setLogLevel("ERROR");

            String lastActionQuery=
                    "SELECT ID, LAST_ACTION, DURATION " +
                    "FROM visit_stats " +
                    "WHERE INTERVAL_TIMESTAMP BETWEEN " +
                    "'" +  startDate + "' AND '" + endDate + "' " +
                    "AND duration > 15";

            Dataset<Row> lastActionDF
                    = spark.read()
                    .format("jdbc")
                    //Using mysql since there is a bug in mariadb connector
                    //https://issues.apache.org/jira/browse/SPARK-25013
                    .option("url", "jdbc:mysql://localhost:3306/website_stats")
                    .option("dbtable", "( " + lastActionQuery + " ) as tmpLastAction")
                    .option("user", "spark")
                    .option("password", "spark")
                    .option("partitionColumn","ID")
                    .option("lowerBound", minBounds)
                    .option("upperBound",maxBounds + 1)
                    .option("numPartitions",2)
                    .load();

            //Write record to Kafka
            lastActionDF.selectExpr("LAST_ACTION as key",
                        "LAST_ACTION as value")
                    .write()
                    .format("kafka")
                    .option("checkpointLocation", "/tmp/cp-lastaction")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("topic", "spark.exercise.lastaction.long")
                    .save();

            System.out.println("Last Action Count for " + startDate + " is " + lastActionDF.count());
            spark.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
