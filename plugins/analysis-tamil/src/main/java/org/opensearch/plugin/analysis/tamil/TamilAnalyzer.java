/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.analysis.tamil;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/**
 * Analyzer for Tamil text using the standard tokenizer and Tamil-specific filters.
 * <p>
 * The analysis chain is:
 * <ol>
 *   <li>{@link StandardTokenizer} - Unicode text segmentation</li>
 *   <li>{@link LowerCaseFilter} - Lowercasing</li>
 *   <li>{@link StopFilter} - Tamil stopword removal</li>
 *   <li>{@link TamilStemmer} - Tamil suffix stripping</li>
 * </ol>
 * <p>
 * This analyzer is dependency-free and uses the standard tokenizer. For production
 * use, consider composing a custom analyzer with {@code icu_tokenizer} and
 * {@code icu_normalizer} from the analysis-icu plugin for better Unicode handling.
 */
public final class TamilAnalyzer extends StopwordAnalyzerBase {

    private final int minStemLength;

    /**
     * Creates a new TamilAnalyzer with the specified stopwords and stem length.
     *
     * @param stopwords the stopword set to use
     * @param minStemLength minimum stem length for the Tamil stemmer
     */
    public TamilAnalyzer(CharArraySet stopwords, int minStemLength) {
        super(stopwords);
        this.minStemLength = minStemLength;
    }

    /**
     * Creates a new TamilAnalyzer with default stopwords and stem length.
     */
    public TamilAnalyzer() {
        this(TamilStopWords.getDefaultStopSet(), 3);
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer source = new StandardTokenizer();
        TokenStream result = new LowerCaseFilter(source);
        result = new StopFilter(result, stopwords);
        result = new TamilStemmer(result, minStemLength, false, true, (java.util.List<String>) null, (java.util.List<String>) null);
        return new TokenStreamComponents(source, result);
    }
}
