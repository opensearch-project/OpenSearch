/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ja.JapaneseCompletionFilter;
import org.apache.lucene.analysis.ja.JapaneseCompletionFilter.Mode;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;

public class KuromojiCompletionFilterFactory extends AbstractTokenFilterFactory {
    private final Mode mode;

    public KuromojiCompletionFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        this.mode = getMode(settings);
    }

    public static Mode getMode(Settings settings) {
        String modeSetting = settings.get("mode", null);
        if (modeSetting != null) {
            if ("index".equalsIgnoreCase(modeSetting)) {
                return Mode.INDEX;
            } else if ("query".equalsIgnoreCase(modeSetting)) {
                return Mode.QUERY;
            }
        }
        return Mode.INDEX;
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new JapaneseCompletionFilter(tokenStream, mode);
    }
}
