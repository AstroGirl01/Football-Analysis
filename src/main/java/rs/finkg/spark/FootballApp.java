package rs.finkg.spark;

import org.apache.spark.api.java.*;
import org.apache.spark.sql.*;
import org.apache.spark.SparkConf;

public class FootballApp {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Football Analysis").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        System.out.println("✅ Spark session started successfully!");

        sc.stop();
    }
}
