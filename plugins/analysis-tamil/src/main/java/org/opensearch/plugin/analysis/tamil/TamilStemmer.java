/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.analysis.tamil;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A token filter that performs prefix and suffix stripping on Tamil tokens.
 * <p>
 * This implements a configurable affix stripping algorithm with:
 * - Longest-first matching for both prefixes and suffixes
 * - Minimum stem length guard to prevent over-stemming
 * - External configuration via text files
 * <p>
 * The affix rules can be loaded from external files or use built-in defaults.
 * This allows customization without recompiling the plugin.
 */
public final class TamilStemmer extends TokenFilter {

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final int minStemLength;
    private final boolean stripPrefixes;
    private final boolean stripSuffixes;
    private final boolean applySandhi;
    private final String[] suffixes;
    private final String[] prefixes;
    private final Map<String, String> sandhiMap;

    private static final String DEFAULT_SUFFIXES_FILE = "tamil_suffixes.txt";
    private static final String DEFAULT_PREFIXES_FILE = "tamil_prefixes.txt";
    private static final String DEFAULT_SANDHI_FILE = "tamil_sandhi.txt";

    /**
     * Creates a new TamilStemmer with default settings.
     *
     * @param input the input token stream
     */
    public TamilStemmer(TokenStream input) {
        this(input, 2, true, true, true, (List<String>) null, (List<String>) null);
    }

    /**
     * Creates a new TamilStemmer with specified min stem length and default affixes.
     *
     * @param input the input token stream
     * @param minStemLength minimum length a stem must have after affix removal
     */
    public TamilStemmer(TokenStream input, int minStemLength) {
        this(input, minStemLength, true, true, true, (List<String>) null, (List<String>) null);
    }

    /**
     * Creates a new TamilStemmer with the specified settings (backwards compatible).
     *
     * @param input the input token stream
     * @param minStemLength minimum length a stem must have after affix removal
     * @param stripPrefixes whether to strip prefixes
     * @param stripSuffixes whether to strip suffixes
     * @param customSuffixes custom suffix list (null to use defaults)
     * @param customPrefixes custom prefix list (null to use defaults)
     */
    public TamilStemmer(TokenStream input, int minStemLength, boolean stripPrefixes,
                        boolean stripSuffixes, List<String> customSuffixes, List<String> customPrefixes) {
        this(input, minStemLength, stripPrefixes, stripSuffixes, true, customSuffixes, customPrefixes);
    }

    /**
     * Creates a new TamilStemmer with the specified settings including sandhi normalization.
     *
     * @param input the input token stream
     * @param minStemLength minimum length a stem must have after affix removal
     * @param stripPrefixes whether to strip prefixes
     * @param stripSuffixes whether to strip suffixes
     * @param applySandhi whether to apply sandhi normalization after stemming
     * @param customSuffixes custom suffix list (null to use defaults)
     * @param customPrefixes custom prefix list (null to use defaults)
     */
    public TamilStemmer(TokenStream input, int minStemLength, boolean stripPrefixes,
                        boolean stripSuffixes, boolean applySandhi,
                        List<String> customSuffixes, List<String> customPrefixes) {
        super(input);
        this.minStemLength = minStemLength;
        this.stripPrefixes = stripPrefixes;
        this.stripSuffixes = stripSuffixes;
        this.applySandhi = applySandhi;

        // Load suffixes
        if (customSuffixes != null && !customSuffixes.isEmpty()) {
            this.suffixes = sortByLengthDescending(customSuffixes);
        } else {
            this.suffixes = loadDefaultAffixes(DEFAULT_SUFFIXES_FILE);
        }

        // Load prefixes
        if (customPrefixes != null && !customPrefixes.isEmpty()) {
            this.prefixes = sortByLengthDescending(customPrefixes);
        } else {
            this.prefixes = loadDefaultAffixes(DEFAULT_PREFIXES_FILE);
        }

        // Load sandhi mappings
        this.sandhiMap = loadSandhiMappings(DEFAULT_SANDHI_FILE);
    }

    /**
     * Creates a TamilStemmer with custom affix readers.
     *
     * @param input the input token stream
     * @param minStemLength minimum stem length
     * @param stripPrefixes whether to strip prefixes
     * @param stripSuffixes whether to strip suffixes
     * @param suffixReader reader for suffix file (null to use defaults)
     * @param prefixReader reader for prefix file (null to use defaults)
     */
    public TamilStemmer(TokenStream input, int minStemLength, boolean stripPrefixes,
                        boolean stripSuffixes, Reader suffixReader, Reader prefixReader) {
        super(input);
        this.minStemLength = minStemLength;
        this.stripPrefixes = stripPrefixes;
        this.stripSuffixes = stripSuffixes;
        this.applySandhi = true;

        // Load suffixes
        if (suffixReader != null) {
            this.suffixes = loadAffixesFromReader(suffixReader);
        } else {
            this.suffixes = loadDefaultAffixes(DEFAULT_SUFFIXES_FILE);
        }

        // Load prefixes
        if (prefixReader != null) {
            this.prefixes = loadAffixesFromReader(prefixReader);
        } else {
            this.prefixes = loadDefaultAffixes(DEFAULT_PREFIXES_FILE);
        }

        // Load sandhi mappings
        this.sandhiMap = loadSandhiMappings(DEFAULT_SANDHI_FILE);
    }

    /**
     * Load affixes from the default resource file bundled with the plugin.
     */
    private String[] loadDefaultAffixes(String filename) {
        List<String> affixes = new ArrayList<>();
        try (InputStream is = getClass().getResourceAsStream(filename)) {
            if (is == null) {
                // Return empty array if file not found
                return new String[0];
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    // Skip empty lines and comments
                    if (!line.isEmpty() && !line.startsWith("#")) {
                        affixes.add(line);
                    }
                }
            }
        } catch (IOException e) {
            // Return empty array on error
            return new String[0];
        }
        return sortByLengthDescending(affixes);
    }

    /**
     * Load affixes from a reader (for custom files).
     */
    private String[] loadAffixesFromReader(Reader reader) {
        List<String> affixes = new ArrayList<>();
        try (BufferedReader bufferedReader = new BufferedReader(reader)) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                line = line.trim();
                // Skip empty lines and comments
                if (!line.isEmpty() && !line.startsWith("#")) {
                    affixes.add(line);
                }
            }
        } catch (IOException e) {
            // Return empty array on error
            return new String[0];
        }
        return sortByLengthDescending(affixes);
    }

    /**
     * Sort affixes by length descending for longest-first matching.
     */
    private String[] sortByLengthDescending(List<String> affixes) {
        return affixes.stream()
            .sorted(Comparator.comparingInt(String::length).reversed())
            .toArray(String[]::new);
    }

    /**
     * Load sandhi mappings from the default resource file.
     * Format: transformed_stem=normalized_stem
     */
    private Map<String, String> loadSandhiMappings(String filename) {
        Map<String, String> mappings = new HashMap<>();
        try (InputStream is = getClass().getResourceAsStream(filename)) {
            if (is == null) {
                return mappings;
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    // Skip empty lines and comments
                    if (!line.isEmpty() && !line.startsWith("#")) {
                        int eqIndex = line.indexOf('=');
                        if (eqIndex > 0) {
                            String key = line.substring(0, eqIndex).trim();
                            String value = line.substring(eqIndex + 1).trim();
                            if (!key.isEmpty() && !value.isEmpty()) {
                                mappings.put(key, value);
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            // Return empty map on error
        }
        return mappings;
    }

    @Override
    public boolean incrementToken() throws IOException {
        if (!input.incrementToken()) {
            return false;
        }
        stem();
        return true;
    }

    private void stem() {
        char[] buffer = termAtt.buffer();
        int length = termAtt.length();
        String term = new String(buffer, 0, length);

        String stemmed = term;

        // Strip prefix first (if enabled)
        if (stripPrefixes && prefixes.length > 0) {
            stemmed = stripPrefix(stemmed);
        }

        // Then strip suffix (if enabled)
        if (stripSuffixes && suffixes.length > 0) {
            stemmed = stripSuffix(stemmed);
        }

        // Apply sandhi normalization (if enabled)
        if (applySandhi && sandhiMap != null && !sandhiMap.isEmpty()) {
            stemmed = applySandhiNormalization(stemmed);
        }

        // Update the term if changed
        if (!stemmed.equals(term)) {
            termAtt.setEmpty().append(stemmed);
        }
    }

    /**
     * Apply sandhi normalization to restore the original root form.
     * First checks for exact match, then checks for suffix-based patterns.
     */
    private String applySandhiNormalization(String stem) {
        // Check for exact match first
        if (sandhiMap.containsKey(stem)) {
            return sandhiMap.get(stem);
        }

        // Check for suffix-based sandhi patterns (longest match first)
        String bestMatch = null;
        String bestReplacement = null;
        int bestLength = 0;

        for (Map.Entry<String, String> entry : sandhiMap.entrySet()) {
            String pattern = entry.getKey();
            if (stem.endsWith(pattern) && pattern.length() > bestLength) {
                bestMatch = pattern;
                bestReplacement = entry.getValue();
                bestLength = pattern.length();
            }
        }

        if (bestMatch != null) {
            // Replace the ending with the normalized form
            String prefix = stem.substring(0, stem.length() - bestMatch.length());
            return prefix + bestReplacement;
        }

        return stem;
    }

    private String stripPrefix(String term) {
        for (String prefix : prefixes) {
            if (term.startsWith(prefix)) {
                String newTerm = term.substring(prefix.length());
                if (newTerm.length() >= minStemLength) {
                    return newTerm;
                }
            }
        }
        return term;
    }

    private String stripSuffix(String term) {
        for (String suffix : suffixes) {
            if (term.endsWith(suffix)) {
                String newTerm = term.substring(0, term.length() - suffix.length());
                if (newTerm.length() >= minStemLength) {
                    return newTerm;
                }
            }
        }
        return term;
    }

    /**
     * Get the loaded suffixes (for testing).
     */
    String[] getSuffixes() {
        return suffixes;
    }

    /**
     * Get the loaded prefixes (for testing).
     */
    String[] getPrefixes() {
        return prefixes;
    }

    /**
     * Get the loaded sandhi mappings (for testing).
     */
    Map<String, String> getSandhiMap() {
        return sandhiMap;
    }
}
