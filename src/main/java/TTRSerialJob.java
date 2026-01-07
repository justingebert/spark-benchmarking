import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.stream.Stream;

/**
 * Serial (Single-Threaded) implementation of TTR calculation.
 * Serves as a performance baseline to compare against Spark.
 */
public class TTRSerialJob {

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("Usage: TTRSerialJob <text-directory> <stopwords-directory>");
            System.exit(1);
        }

        String textDir = args[0];
        String stopwordsDir = args[1];

        System.out.println("=".repeat(60));
        System.out.println("Type-Token-Ratio Serial Analysis");
        System.out.println("Text directory: " + textDir);
        System.out.println("Stopwords directory: " + stopwordsDir);
        System.out.println("=".repeat(60));

        long startTime = System.currentTimeMillis();

        // Load stopwords using shared util
        Map<String, Set<String>> stopwordsMap = TTRUtils.loadAllStopwords(stopwordsDir);

        // Stats storage: Language -> (Total, Unique Set)
        Map<String, Set<String>> uniqueTokens = new HashMap<>();
        Map<String, Long> totalTokens = new HashMap<>();

        try (Stream<Path> paths = Files.walk(Paths.get(textDir))) {
            paths.filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".txt"))
                    .forEach(path -> processFile(path, stopwordsMap, uniqueTokens, totalTokens));
        }

        long endTime = System.currentTimeMillis();
        double executionTime = (endTime - startTime) / 1000.0;

        printResults(uniqueTokens, totalTokens);

        System.out.println("-".repeat(60));
        System.out.printf("Execution time: %.2f seconds%n", executionTime);
        System.out.println("=".repeat(60));
    }

    private static void processFile(Path path, Map<String, Set<String>> stopwordsMap,
            Map<String, Set<String>> uniqueTokens,
            Map<String, Long> totalTokens) {

        // Use shared language extraction
        String language = TTRUtils.extractLanguage(path.toString());
        if (language == null)
            return;

        Set<String> stopWords = stopwordsMap.getOrDefault(language, Collections.emptySet());

        // Initialize maps if needed
        // Note: Not thread-safe, but this is a serial job
        uniqueTokens.putIfAbsent(language, new HashSet<>());
        totalTokens.putIfAbsent(language, 0L);

        try {
            String content = Files.readString(path);
            // Use shared pattern
            Matcher matcher = TTRUtils.WORD_PATTERN.matcher(content);

            long count = 0;
            Set<String> unique = uniqueTokens.get(language);

            while (matcher.find()) {
                // Use shared normalization
                String word = TTRUtils.normalizeWord(matcher.group());

                if (!stopWords.contains(word)) {
                    count++;
                    unique.add(word);
                }
            }

            totalTokens.put(language, totalTokens.get(language) + count);

        } catch (IOException e) {
            System.err.println("Error reading file " + path + ": " + e.getMessage());
        }
    }

    private static void printResults(Map<String, Set<String>> uniqueTokens, Map<String, Long> totalTokens) {
        System.out.println();
        System.out.println("=".repeat(60));
        System.out.println("RESULTS: Type-Token-Ratio per Language (Serial)");
        System.out.println("=".repeat(60));
        System.out.printf("%-12s %15s %15s %10s%n", "Language", "Total Tokens", "Unique Tokens", "TTR");
        System.out.println("-".repeat(60));

        totalTokens.keySet().stream().sorted().forEach(lang -> {
            long total = totalTokens.get(lang);
            long unique = uniqueTokens.get(lang).size();
            double ttr = (double) unique / total;
            System.out.printf("%-12s %,15d %,15d %10.4f%n", lang, total, unique, ttr);
        });
    }
}
