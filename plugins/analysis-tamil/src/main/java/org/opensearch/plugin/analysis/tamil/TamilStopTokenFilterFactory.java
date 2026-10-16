/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.analysis.tamil;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AbstractTokenFilterFactory;
import org.opensearch.index.analysis.Analysis;

/**
 * Factory for creating a Tamil stopword filter.
 * <p>
 * Configuration options:
 * <ul>
 *   <li>{@code stopwords} - An inline array of stopwords</li>
 *   <li>{@code stopwords_path} - Path to a file containing stopwords (one per line)</li>
 * </ul>
 * If neither is specified, uses the bundled default Tamil stopword set from
 * {@link TamilStopWords#getDefaultStopSet()}.
 */
public class TamilStopTokenFilterFactory extends AbstractTokenFilterFactory {

    private final CharArraySet stopwords;

    /**
     * Creates a new TamilStopTokenFilterFactory.
     *
     * @param indexSettings the index settings
     * @param env the environment
     * @param name the filter name
     * @param settings the filter settings
     */
    public TamilStopTokenFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);
        this.stopwords = Analysis.parseStopWords(env, settings, TamilStopWords.getDefaultStopSet());
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new StopFilter(tokenStream, stopwords);
    }
}
