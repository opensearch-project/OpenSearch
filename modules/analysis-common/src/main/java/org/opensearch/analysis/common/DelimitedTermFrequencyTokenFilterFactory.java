/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.DelimitedTermFrequencyTokenFilter;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AbstractTokenFilterFactory;

public class DelimitedTermFrequencyTokenFilterFactory extends AbstractTokenFilterFactory {
    public static final char DEFAULT_DELIMITER = '|';
    private static final String DELIMITER = "delimiter";
    private final char delimiter;

    DelimitedTermFrequencyTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        delimiter = parseDelimiter(settings);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new DelimitedTermFrequencyTokenFilter(tokenStream, delimiter);
    }

    private static char parseDelimiter(Settings settings) {
        String delimiter = settings.get(DELIMITER);
        if (delimiter == null) {
            return DEFAULT_DELIMITER;
        } else if (delimiter.length() == 1) {
            return delimiter.charAt(0);
        }

        throw new IllegalArgumentException(
            "Setting [" + DELIMITER + "] must be a single, non-null character. [" + delimiter + "] was provided."
        );
    }
}
