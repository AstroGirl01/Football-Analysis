package rs.finkg.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

public class UefaEuroHatTricks {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("UEFA EURO Hat-Tricks (RDD Join)")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

       
        String resultsPath     = "D:/Fakultet/Master studije/Distribuirane mreze i sistemi/Apache Spark/Football-Analysis/src/main/resources/results.csv";
        String goalscorersPath = "D:/Fakultet/Master studije/Distribuirane mreze i sistemi/Apache Spark/Football-Analysis/src/main/resources/goalscorers.csv";

     
        JavaRDD<String> resLines = sc.textFile(resultsPath);
        String resHeader = resLines.first();
        JavaRDD<String> results = resLines.filter(l -> !l.equals(resHeader));

        JavaRDD<String> gsLines = sc.textFile(goalscorersPath);
        String gsHeader = gsLines.first();
        JavaRDD<String> goals = gsLines.filter(l -> !l.equals(gsHeader));



        JavaPairRDD<String, String> matchToTournament = results.mapToPair(line -> {
            String[] p = splitCsv(line);
            if (p.length < 9) return new Tuple2<>("", ""); // skip
            String key = (p[0].trim() + "|" + p[1].trim() + "|" + p[2].trim());
            String tournament = p[5].trim();
            return new Tuple2<>(key, tournament);
        }).filter(t -> !t._1.isEmpty());


        JavaPairRDD<String, String> euroMatches = matchToTournament
                .filter(t -> t._2.equalsIgnoreCase("UEFA Euro"));


        JavaPairRDD<String, String> matchToScorer = goals.mapToPair(line -> {
            String[] p = splitCsv(line);
            if (p.length < 8) return new Tuple2<>("", "");
            String key = (p[0].trim() + "|" + p[1].trim() + "|" + p[2].trim());
            String scorer = p[4].trim();
            return new Tuple2<>(key, scorer);
        }).filter(t -> !t._1.isEmpty() && !t._2.isEmpty());

       
        JavaPairRDD<String, Tuple2<String, String>> joined = matchToScorer.join(euroMatches);


       
        JavaPairRDD<String, Integer> goalsPerPlayerPerMatch = joined
                .mapToPair(t -> new Tuple2<>(t._1 + "|" + t._2._1, 1)) 
                .reduceByKey(Integer::sum)
                .filter(t -> t._2 == 3);

       
        List<String> pretty = goalsPerPlayerPerMatch.keys().map(k -> {
            String[] parts = k.split("\\|");

            if (parts.length < 4) return "";
            return "🎯 " + parts[0] + " | " + parts[1] + " vs " + parts[2] + " | " + parts[3];
        }).filter(s -> !s.isEmpty()).collect();

        System.out.println("==============================================");
        System.out.println("🏆 HET-TRIKOVI NA UEFA EURO TURNIRIMA:");
        System.out.println("==============================================");
        if (pretty.isEmpty()) {
            System.out.println("Nema het-trikova pronađenih u datasetu.");
        } else {
            pretty.forEach(System.out::println);
        }
        System.out.println("==============================================");

        sc.stop();
    }

   
    private static String[] splitCsv(String line) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                out.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        out.add(cur.toString());
        return out.toArray(new String[0]);
    }
}
