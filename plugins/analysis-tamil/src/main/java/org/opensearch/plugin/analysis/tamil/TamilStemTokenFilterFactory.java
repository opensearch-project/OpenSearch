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
import org.opensearch.index.analysis.Analysis;

import java.io.Reader;
import java.util.List;

/**
 * Factory for {@link TamilStemmer}.
 * <p>
 * Configuration options:
 * <ul>
 *   <li>{@code min_stem_length} - Minimum length a stem must have after affix removal.
 *       Default is 2 (measured in Java chars). Setting this too low risks over-stemming.</li>
 *   <li>{@code strip_prefixes} - Whether to strip prefixes. Default is true.</li>
 *   <li>{@code strip_suffixes} - Whether to strip suffixes. Default is true.</li>
 *   <li>{@code suffixes_path} - Path to custom suffixes file. Uses built-in defaults if not specified.</li>
 *   <li>{@code prefixes_path} - Path to custom prefixes file. Uses built-in defaults if not specified.</li>
 *   <li>{@code suffixes} - Inline list of suffixes. Overrides suffixes_path if specified.</li>
 *   <li>{@code prefixes} - Inline list of prefixes. Overrides prefixes_path if specified.</li>
 * </ul>
 * <p>
 * Example configuration:
 * <pre>
 * {
 *   "filter": {
 *     "tamil_stem": {
 *       "type": "tamil_stem",
 *       "min_stem_length": 2,
 *       "strip_prefixes": true,
 *       "strip_suffixes": true,
 *       "suffixes_path": "analysis/my_suffixes.txt"
 *     }
 *   }
 * }
 * </pre>
 */
public class TamilStemTokenFilterFactory extends AbstractTokenFilterFactory {

    private static final int DEFAULT_MIN_STEM_LENGTH = 2;

    private final int minStemLength;
    private final boolean stripPrefixes;
    private final boolean stripSuffixes;
    private final List<String> suffixes;
    private final List<String> prefixes;
    private final Reader suffixReader;
    private final Reader prefixReader;

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
        this.stripPrefixes = settings.getAsBoolean("strip_prefixes", false);
        this.stripSuffixes = settings.getAsBoolean("strip_suffixes", true);

        // Check for inline suffixes first, then file path
        this.suffixes = settings.getAsList("suffixes", null);
        if (this.suffixes == null || this.suffixes.isEmpty()) {
            this.suffixReader = Analysis.getReaderFromFile(env, settings, "suffixes_path");
        } else {
            this.suffixReader = null;
        }

        // Check for inline prefixes first, then file path
        this.prefixes = settings.getAsList("prefixes", null);
        if (this.prefixes == null || this.prefixes.isEmpty()) {
            this.prefixReader = Analysis.getReaderFromFile(env, settings, "prefixes_path");
        } else {
            this.prefixReader = null;
        }
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        // If we have readers from files, use the reader constructor
        if (suffixReader != null || prefixReader != null) {
            return new TamilStemmer(tokenStream, minStemLength, stripPrefixes, stripSuffixes,
                                   suffixReader, prefixReader);
        }
        // Otherwise use the list constructor (which falls back to defaults if lists are null/empty)
        return new TamilStemmer(tokenStream, minStemLength, stripPrefixes, stripSuffixes,
                               suffixes, prefixes);
    }
}
