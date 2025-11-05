package rs.finkg.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import scala.Tuple2;

import java.util.*;

public class SerbiaGoalDifference {
    public static void main(String[] args) {
        
        SparkConf conf = new SparkConf()
                .setAppName("Serbia Goal Difference Analysis")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

      
        String path = "src/main/resources/results.csv";

      
        JavaRDD<String> lines = sc.textFile(path);


        String header = lines.first();
        JavaRDD<String> data = lines.filter(line -> !line.equals(header));

       
        JavaPairRDD<String, Integer> differences = data.flatMapToPair(line -> {
            List<Tuple2<String, Integer>> result = new ArrayList<>();
            String[] parts = line.split(",");

            if (parts.length < 8) return result.iterator(); 

            String home = parts[1].trim();
            String away = parts[2].trim();
            int homeScore = Integer.parseInt(parts[3].trim());
            int awayScore = Integer.parseInt(parts[4].trim());

            if (home.equals("Serbia")) {
                int diff = homeScore - awayScore;
                result.add(new Tuple2<>(away, diff));
            } else if (away.equals("Serbia")) {
                int diff = awayScore - homeScore;
                result.add(new Tuple2<>(home, diff));
            }

            return result.iterator();
        });


        JavaPairRDD<String, Integer> totalDiffs = differences.reduceByKey(Integer::sum);

       
        Tuple2<String, Integer> best = totalDiffs.max(new TupleComparator());
        Tuple2<String, Integer> worst = totalDiffs.min(new TupleComparator());

        System.out.println("---------------------------------------------------");
        System.out.println(" Najbolja gol razlika Srbije je protiv: " + best._1 + " (" + best._2 + ")");
        System.out.println(" Najgora gol razlika Srbije je protiv: " + worst._1 + " (" + worst._2 + ")");
        System.out.println("---------------------------------------------------");

        sc.stop();
    }

   
    static class TupleComparator implements Comparator<Tuple2<String, Integer>>, java.io.Serializable {
        public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
            return Integer.compare(t1._2, t2._2);
        }
    }
}
