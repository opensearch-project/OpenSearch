/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.analysis.tamil;

import org.apache.lucene.analysis.TokenStream;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AbstractTokenFilterFactory;

/**
 * Factory for {@link TamilStemmer}.
 * <p>
 * Configuration options:
 * <ul>
 *   <li>{@code min_stem_length} - Minimum length a stem must have after suffix removal.
 *       Default is 3 (measured in Java chars, which for Tamil typically means 3 aksharas).
 *       Setting this too low risks over-stemming short words.</li>
 * </ul>
 */
public class TamilStemTokenFilterFactory extends AbstractTokenFilterFactory {

    private static final int DEFAULT_MIN_STEM_LENGTH = 3;

    private final int minStemLength;

    /**
     * Creates a new TamilStemTokenFilterFactory.
     *
     * @param indexSettings the index settings
     * @param env the environment
     * @param name the filter name
     * @param settings the filter settings
     */
    public TamilStemTokenFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);
        this.minStemLength = settings.getAsInt("min_stem_length", DEFAULT_MIN_STEM_LENGTH);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new TamilStemmer(tokenStream, minStemLength);
    }
}
