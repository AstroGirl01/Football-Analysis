package rs.finkg.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

public class WorldCupTopScorers {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("World Cup Top Scorers (Window + Join)")
                .master("local[*]")
                .getOrCreate();

        String base = "D:/Fakultet/Master studije/Distribuirane mreze i sistemi/Apache Spark/Football-Analysis/src/main/resources/";
        String resultsPath     = base + "results.csv";
        String goalscorersPath = base + "goalscorers.csv";


        Dataset<Row> resultsDF = spark.read()
                .option("header", "true").option("inferSchema", "true")
                .option("multiLine", "true").option("mode", "PERMISSIVE")
                .csv(resultsPath)
                .withColumn("date", to_date(col("date")))
                .withColumn("year", year(col("date")));

       
        Dataset<Row> goalscorersDF = spark.read()
                .option("header", "true").option("inferSchema", "true")
                .option("multiLine", "true").option("mode", "PERMISSIVE")
                .csv(goalscorersPath)
                .withColumn("date", to_date(col("date")))
                .withColumn("minute", col("minute").cast("int"));

        
        Column joinCond = goalscorersDF.col("date").equalTo(resultsDF.col("date"))
                .and(goalscorersDF.col("home_team").equalTo(resultsDF.col("home_team")))
                .and(goalscorersDF.col("away_team").equalTo(resultsDF.col("away_team")));

        Dataset<Row> joined = goalscorersDF.join(resultsDF, joinCond, "inner")
                .select(
                        goalscorersDF.col("scorer"),
                        goalscorersDF.col("team"),
                        resultsDF.col("tournament"),
                        resultsDF.col("country"),
                        resultsDF.col("year")
                )
                .filter(col("tournament").equalTo("FIFA World Cup").and(col("year").geq(1990)));

        
        Dataset<Row> goalsPerYear = joined.groupBy(col("year"), col("scorer"), col("team"),
                                                   col("tournament"), col("country"))
                .agg(count(lit(1)).alias("totalGoals"));

    
        WindowSpec w = Window.partitionBy("year").orderBy(col("totalGoals").desc(), col("scorer").asc());
        Dataset<Row> ranked = goalsPerYear.withColumn("rang", rank().over(w));

       
        Dataset<Row> top12 = ranked.filter(col("rang").leq(2))
                .orderBy(col("year").asc(), col("totalGoals").desc());

        
        top12.show(200, false);

        spark.stop();
    }
}
