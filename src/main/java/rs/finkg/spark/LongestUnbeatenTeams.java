package rs.finkg.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

public class LongestUnbeatenTeams {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Longest Unbeaten Teams (Window + Join)")
                .master("local[*]")
                .getOrCreate();


        String base = "D:/Fakultet/Master studije/Distribuirane mreze i sistemi/Apache Spark/Football-Analysis/src/main/resources/";
        String resultsPath = base + "results.csv";


        Dataset<Row> resultsDF = spark.read()
                .option("header", "true").option("inferSchema", "true")
                .option("multiLine", "true").option("mode", "PERMISSIVE")
                .csv(resultsPath)
                .withColumn("date", to_date(col("date")));


        Dataset<Row> wcTeams = resultsDF.filter(col("tournament").equalTo("FIFA World Cup"))
                .select(col("home_team").alias("team")).union(
                        resultsDF.filter(col("tournament").equalTo("FIFA World Cup"))
                                .select(col("away_team").alias("team"))
                ).distinct();


        Dataset<Row> after1980 = resultsDF.filter(col("date").geq(lit("1980-01-01")));

        
        Dataset<Row> homeView = after1980.select(
                col("date"),
                col("home_team").alias("team"),
                col("away_team").alias("opponent"),
                when(col("home_score").gt(col("away_score")), lit("W"))
                        .when(col("home_score").lt(col("away_score")), lit("L"))
                        .otherwise(lit("D")).alias("outcome")
        );

        Dataset<Row> awayView = after1980.select(
                col("date"),
                col("away_team").alias("team"),
                col("home_team").alias("opponent"),
                when(col("away_score").gt(col("home_score")), lit("W"))
                        .when(col("away_score").lt(col("home_score")), lit("L"))
                        .otherwise(lit("D")).alias("outcome")
        );

        Dataset<Row> teamMatches = homeView.union(awayView);

        
        Dataset<Row> eligible = teamMatches.join(wcTeams, "team");

        
        Dataset<Row> losses = eligible.filter(col("outcome").equalTo("L"))
                .select(col("team"), col("opponent"), col("date"));

        WindowSpec w = Window.partitionBy("team").orderBy(col("date").asc());
        Dataset<Row> withPrev = losses
                .withColumn("prev_loss_date", lag(col("date"), 1).over(w))
                .withColumn("prev_opponent", lag(col("opponent"), 1).over(w));

       
        Dataset<Row> streaks = withPrev.filter(col("prev_loss_date").isNotNull())
                .withColumn("period_dana", datediff(col("date"), col("prev_loss_date")))
                .select(
                        col("team").alias("Reprezentacija"),
                        col("opponent").alias("Poraz_od"),
                        col("date").alias("Poraz_Datum"),
                        col("prev_opponent").alias("Prethodni_poraz_od"),
                        col("prev_loss_date").alias("Prethodni_poraz_datum"),
                        col("period_dana").alias("Period_dana")
                );

        
        Dataset<Row> top20 = streaks.orderBy(col("Period_dana").desc(), col("Reprezentacija").asc())
                .limit(20);

        
        top20.show(200, false);

        spark.stop();
    }
}
