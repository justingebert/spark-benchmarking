import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;

/**
 * Spark job to calculate Type-Token-Ratio (TTR) per language.
 * TTR = unique tokens / total tokens
 * 
 * Usage: TTRJob <text-directory> <stopwords-directory> [output-directory]
 * 
 * @authors Justin Gebert, Ole Lordieck
 */
public final class TTRJob implements Serializable {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: TTRJob <text-directory> <stopwords-directory> [output-directory]");
            System.exit(1);
        }

        String textDir = args[0];
        String stopwordsDir = args[1];
        String outputDir = args.length > 2 ? args[2] : "results";

        // setup spark
        SparkConf conf = new SparkConf()
                .setAppName("TTR-TypeTokenRatio");
        // Master is set via spark-submit

        JavaSparkContext sc = new JavaSparkContext(conf);

        System.out.println("=".repeat(60));
        System.out.println("Type-Token-Ratio Analysis");
        System.out.println("Spark version: " + sc.version());
        System.out.println("Text directory: " + textDir);
        System.out.println("Stopwords directory: " + stopwordsDir);
        System.out.println("=".repeat(60));

        long startTime = System.currentTimeMillis();

        // Load all stopwords into a map: language -> Set<stopword>
        Map<String, Set<String>> stopwordsMap = TTRUtils.loadAllStopwords(stopwordsDir);

        // Broadcast stopwords to all workers for efficient access
        Broadcast<Map<String, Set<String>>> broadcastStopwords = sc.broadcast(stopwordsMap);

        // Read all text files with their paths: (path, content)  - 8 partitions because 8 is max cores - keeps it fair
        JavaPairRDD<String, String> filesRDD = sc.wholeTextFiles(textDir + "/*/*.txt", 8);

        System.out.println("Files loaded: " + filesRDD.count());

        // Extract (language, word) pairs from files
        JavaPairRDD<String, String> langWordPairs = filesRDD.flatMapToPair(fileTuple -> {
            String path = fileTuple._1();
            String content = fileTuple._2();

            //extractLanguage
            String language = TTRUtils.extractLanguage(path);
            if (language == null) {
                return Collections.emptyIterator();
            }

            // Get stopwords for this language
            Set<String> stopwords = broadcastStopwords.value().getOrDefault(language, Collections.emptySet());

            List<Tuple2<String, String>> pairs = new ArrayList<>();

            Matcher matcher = TTRUtils.WORD_PATTERN.matcher(content);

            while (matcher.find()) {
                // normalization
                String word = TTRUtils.normalizeWord(matcher.group());

                if (!stopwords.contains(word)) {
                    pairs.add(new Tuple2<>(language, word));
                }
            }

            return pairs.iterator();
        });

        // Cache the RDD since we use it twice
        langWordPairs.cache();

        // Count total tokens per language
        JavaPairRDD<String, Long> totalTokensPerLang = langWordPairs
                .mapToPair(t -> new Tuple2<>(t._1(), 1L))
                .reduceByKey(Long::sum);

        // Count unique tokens per language
        // First get distinct (language, word) pairs, then count by language
        JavaPairRDD<String, Long> uniqueTokensPerLang = langWordPairs
                .distinct()
                .mapToPair(t -> new Tuple2<>(t._1(), 1L))
                .reduceByKey(Long::sum);

        // Join total and unique counts
        JavaPairRDD<String, Tuple2<Long, Long>> joinedCounts = totalTokensPerLang.join(uniqueTokensPerLang);

        // Calculate TTR and collect results
        List<Tuple2<String, double[]>> results = new ArrayList<>(joinedCounts
                .mapToPair(t -> {
                    String lang = t._1();
                    long total = t._2()._1();
                    long unique = t._2()._2();
                    double ttr = (double) unique / total;
                    return new Tuple2<>(lang, new double[] { total, unique, ttr });
                })
                .collect());

        // Sort results by language name
        results.sort(Comparator.comparing(Tuple2::_1));

        long endTime = System.currentTimeMillis();
        double executionTime = (endTime - startTime) / 1000.0;

        // Print results table
        System.out.println();
        System.out.println("=".repeat(60));
        System.out.println("RESULTS: Type-Token-Ratio per Language");
        System.out.println("=".repeat(60));
        System.out.printf("%-12s %15s %15s %10s%n", "Language", "Total Tokens", "Unique Tokens", "TTR");
        System.out.println("-".repeat(60));

        for (Tuple2<String, double[]> result : results) {
            String lang = result._1();
            long total = (long) result._2()[0];
            long unique = (long) result._2()[1];
            double ttr = result._2()[2];
            System.out.printf("%-12s %,15d %,15d %10.4f%n", lang, total, unique, ttr);
        }

        System.out.println("-".repeat(60));
        System.out.printf("Execution time: %.2f seconds%n", executionTime);
        System.out.println("=".repeat(60));

        // Save results to CSV
        StringBuilder csv = new StringBuilder();
        csv.append("language,total_tokens,unique_tokens,ttr\n");
        for (Tuple2<String, double[]> result : results) {
            csv.append(String.format("%s,%d,%d,%.6f%n",
                    result._1(),
                    (long) result._2()[0],
                    (long) result._2()[1],
                    result._2()[2]));
        }

        // Write CSV to output directory
        String csvPath = outputDir + "/ttr_results.csv";
        Files.createDirectories(Paths.get(outputDir));
        Files.writeString(Paths.get(csvPath), csv.toString());
        System.out.println("Results saved to: " + csvPath);

        // Clean up
        langWordPairs.unpersist();
        sc.stop();
    }
}
