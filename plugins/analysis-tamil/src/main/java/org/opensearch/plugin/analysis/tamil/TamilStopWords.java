/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.analysis.tamil;

import org.apache.lucene.analysis.CharArraySet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Provides the default Tamil stopword set for the tamil_stop filter.
 * <p>
 * The stopwords are loaded from the resource file tamil_stop.txt bundled with
 * this plugin. The list was derived from frequency analysis of Tamil corpora
 * and represents common low-information words suitable for filtering in
 * search/retrieval contexts.
 */
public final class TamilStopWords {

    private static final CharArraySet DEFAULT_STOP_SET;

    static {
        try {
            DEFAULT_STOP_SET = loadStopWords();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load Tamil stopwords", e);
        }
    }

    private TamilStopWords() {}

    /**
     * Returns an unmodifiable CharArraySet containing the default Tamil stopwords.
     *
     * @return the default Tamil stopword set
     */
    public static CharArraySet getDefaultStopSet() {
        return DEFAULT_STOP_SET;
    }

    private static CharArraySet loadStopWords() throws IOException {
        List<String> words = new ArrayList<>();
        try (
            InputStream is = TamilStopWords.class.getResourceAsStream("tamil_stop.txt");
            BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty() && !line.startsWith("#")) {
                    words.add(line);
                }
            }
        }
        return CharArraySet.unmodifiableSet(new CharArraySet(words, false));
    }
}
