import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.Normalizer;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Shared utility methods for TTR calculation to ensure
 * consistency between Spark and Serial implementations.
 */
public class TTRUtils {

    // Unicode pattern to match word characters (supports Cyrillic, Latin, etc.)
    public static final Pattern WORD_PATTERN = Pattern.compile("\\p{L}+");

    // Map language directory names to stopword file codes
    public static final Map<String, String> LANG_TO_STOPWORD_CODE = new HashMap<>();
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

    /**
     * Extract language name from file path.
     * Supports both URI format (file:/...) and OS paths.
     * Checks if any path segment corresponds to a known language key.
     */
    public static String extractLanguage(String path) {
        // Handle Spark's wholeTextFiles URI format
        if (path.startsWith("file:")) {
            path = path.replaceFirst("^file:", "");
        }

        String[] parts = path.split("/");
        for (String part : parts) {
            if (LANG_TO_STOPWORD_CODE.containsKey(part)) {
                return part;
            }
        }
        return null;
    }

    /**
     * Normalize a word: lowercase and Unicode normalization (NFC).
     */
    public static String normalizeWord(String word) {
        String normalized = Normalizer.normalize(word, Normalizer.Form.NFC);
        return normalized.toLowerCase();
    }

    /**
     * Load all stopword files from the stopwords directory.
     * Returns a map: language name -> Set of stopwords
     */
    public static Map<String, Set<String>> loadAllStopwords(String stopwordsDir) {
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
     * Expects format: ["word1", "word2", ...]
     */
    public static Set<String> parseJsonArray(String json) {
        Set<String> words = new HashSet<>();
        json = json.trim();
        if (json.startsWith("["))
            json = json.substring(1);
        if (json.endsWith("]"))
            json = json.substring(0, json.length() - 1);

        Pattern pattern = Pattern.compile("\"([^\"]+)\"");
        Matcher matcher = pattern.matcher(json);
        while (matcher.find()) {
            words.add(matcher.group(1).toLowerCase());
        }
        return words;
    }
}
