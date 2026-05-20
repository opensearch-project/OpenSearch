/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.analysis;

import org.apache.lucene.analysis.ja.JapaneseCompletionAnalyzer;
import org.apache.lucene.analysis.ja.JapaneseCompletionFilter;
import org.apache.lucene.analysis.ja.dict.UserDictionary;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;

public class KuromojiCompletionAnalyzerProvider extends AbstractIndexAnalyzerProvider<JapaneseCompletionAnalyzer> {

    private final JapaneseCompletionAnalyzer analyzer;

    public KuromojiCompletionAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);
        final JapaneseCompletionFilter.Mode mode = KuromojiCompletionFilterFactory.getMode(settings);
        final UserDictionary userDictionary = KuromojiTokenizerFactory.getUserDictionary(env, settings);
        analyzer = new JapaneseCompletionAnalyzer(userDictionary, mode);
    }

    @Override
    public JapaneseCompletionAnalyzer get() {
        return this.analyzer;
    }

}
