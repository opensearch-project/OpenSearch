/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.analysis.tamil;

import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AbstractIndexAnalyzerProvider;

/**
 * Provider for the prebuilt {@code tamil} analyzer.
 * <p>
 * The analyzer uses the standard tokenizer (dependency-free) and applies:
 * <ol>
 *   <li>Lowercase filter</li>
 *   <li>Tamil stopword filter (tamil_stop)</li>
 *   <li>Tamil stemmer filter (tamil_stemmer)</li>
 * </ol>
 * <p>
 * For production use with better Unicode handling and tokenization,
 * compose a custom analyzer using {@code icu_tokenizer} and {@code icu_normalizer}
 * from the analysis-icu plugin instead of this prebuilt analyzer.
 */
public class TamilAnalyzerProvider extends AbstractIndexAnalyzerProvider<TamilAnalyzer> {

    private final TamilAnalyzer analyzer;

    /**
     * Creates a new TamilAnalyzerProvider.
     *
     * @param indexSettings the index settings
     * @param env the environment
     * @param name the analyzer name
     * @param settings the analyzer settings
     */
    public TamilAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);
        int minStemLength = settings.getAsInt("min_stem_length", 3);
        this.analyzer = new TamilAnalyzer(TamilStopWords.getDefaultStopSet(), minStemLength);
    }

    @Override
    public TamilAnalyzer get() {
        return this.analyzer;
    }
}
