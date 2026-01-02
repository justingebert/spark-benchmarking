import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.Normalizer;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Spark job to calculate Type-Token-Ratio (TTR) per language.
 * TTR = unique tokens / total tokens
 * 
 * Usage: TTRJob <text-directory> <stopwords-directory> [output-directory]
 * 
 * @authors Justin Gebert, Ole Lordieck
 */
public final class TTRJob implements Serializable {

    // Unicode pattern to match word characters (supports Cyrillic, Latin, etc.)
    private static final Pattern WORD_PATTERN = Pattern.compile("\\p{L}+");

    // Map language directory names to stopword file codes
    private static final Map<String, String> LANG_TO_STOPWORD_CODE = new HashMap<>();
    static {
        LANG_TO_STOPWORD_CODE.put("german", "de");
        LANG_TO_STOPWORD_CODE.put("english", "en");
        LANG_TO_STOPWORD_CODE.put("french", "fr");
        LANG_TO_STOPWORD_CODE.put("spanish", "es");
        LANG_TO_STOPWORD_CODE.put("italian", "it");
        LANG_TO_STOPWORD_CODE.put("dutch", "nl");
        LANG_TO_STOPWORD_CODE.put("russian", "ru");
        LANG_TO_STOPWORD_CODE.put("ukrainian", "ua");
    }

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
        Map<String, Set<String>> stopwordsMap = loadAllStopwords(stopwordsDir);

        // Broadcast stopwords to all workers for efficient access
        Broadcast<Map<String, Set<String>>> broadcastStopwords = sc.broadcast(stopwordsMap);

        // Read all text files with their paths: (path, content)
        JavaPairRDD<String, String> filesRDD = sc.wholeTextFiles(textDir + "/*/*.txt", 8);

        System.out.println("Files loaded: " + filesRDD.count());

        // Extract (language, word) pairs from files
        JavaPairRDD<String, String> langWordPairs = filesRDD.flatMapToPair(fileTuple -> {
            String path = fileTuple._1();
            String content = fileTuple._2();

            String language = extractLanguage(path);
            if (language == null) {
                return Collections.emptyIterator();
            }

            // Get stopwords for this language
            Set<String> stopwords = broadcastStopwords.value().getOrDefault(language, Collections.emptySet());

            // Tokenize content
            List<Tuple2<String, String>> pairs = new ArrayList<>();
            Matcher matcher = WORD_PATTERN.matcher(content);

            while (matcher.find()) {
                String word = normalizeWord(matcher.group());

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

    /**
     * Extract language name from file path.
     * Expected format: .../data/text/<language>/filename.txt
     */
    private static String extractLanguage(String path) {
        // Remove file:// prefix if present
        path = path.replaceFirst("^file:", "");

        String[] parts = path.split("/");
        // Find "text" in path and get the next element
        for (int i = 0; i < parts.length - 1; i++) {
            if ("text".equals(parts[i]) && i + 1 < parts.length) {
                String lang = parts[i + 1];
                if (LANG_TO_STOPWORD_CODE.containsKey(lang)) {
                    return lang;
                }
            }
        }

        // Fallback: try second-to-last element (parent directory of .txt file)
        if (parts.length >= 2) {
            String lang = parts[parts.length - 2];
            if (LANG_TO_STOPWORD_CODE.containsKey(lang)) {
                return lang;
            }
        }

        return null;
    }

    /**
     * Normalize a word: lowercase and Unicode normalization.
     */
    private static String normalizeWord(String word) {
        // Normalize Unicode (handle different representations of same character)
        String normalized = Normalizer.normalize(word, Normalizer.Form.NFC);
        return normalized.toLowerCase();
    }

    /**
     * Load all stopword files from the stopwords directory.
     * Returns a map: language name -> Set of stopwords
     */
    private static Map<String, Set<String>> loadAllStopwords(String stopwordsDir) throws Exception {
        Map<String, Set<String>> result = new HashMap<>();

        for (Map.Entry<String, String> entry : LANG_TO_STOPWORD_CODE.entrySet()) {
            String langName = entry.getKey();
            String langCode = entry.getValue();
            String filePath = stopwordsDir + "/" + langCode + ".json";

            try {
                String content = Files.readString(Paths.get(filePath));
                Set<String> stopwords = parseJsonArray(content);
                result.put(langName, stopwords);
                System.out.println("Loaded " + stopwords.size() + " stopwords for " + langName);
            } catch (Exception e) {
                System.err.println("Warning: Could not load stopwords for " + langName + ": " + e.getMessage());
                result.put(langName, Collections.emptySet());
            }
        }

        return result;
    }

    /**
     * Simple JSON array parser for stopwords.
     * not as robust as jackson or else but reduces dependencies and bloat
     * Expects format: ["word1", "word2", ...]
     */
    private static Set<String> parseJsonArray(String json) {
        Set<String> words = new HashSet<>();

        // Remove brackets and split by comma
        json = json.trim();
        if (json.startsWith("["))
            json = json.substring(1);
        if (json.endsWith("]"))
            json = json.substring(0, json.length() - 1);

        // Parse each quoted string
        Pattern pattern = Pattern.compile("\"([^\"]+)\"");
        Matcher matcher = pattern.matcher(json);
        while (matcher.find()) {
            words.add(matcher.group(1).toLowerCase());
        }

        return words;
    }
}
