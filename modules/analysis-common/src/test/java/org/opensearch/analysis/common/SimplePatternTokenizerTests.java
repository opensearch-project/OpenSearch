/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analysis.common;

import org.apache.lucene.analysis.Tokenizer;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTokenStreamTestCase;

import java.io.IOException;
import java.io.StringReader;

public class SimplePatternTokenizerTests extends OpenSearchTokenStreamTestCase {

    public void testComplexRegexRequiringDeterminization() throws IOException {
        Settings settings = Settings.builder().put("pattern", "(a+|b+)*c").build();
        SimplePatternTokenizerFactory factory = new SimplePatternTokenizerFactory(
            IndexSettingsModule.newIndexSettings(new Index("test", "_na_"), Settings.EMPTY),
            null,
            "test",
            settings
        );

        Tokenizer tokenizer = factory.create();
        tokenizer.setReader(new StringReader("aaac bbbbc ac"));
        assertTokenStreamContents(tokenizer, new String[] { "aaac", "bbbbc", "ac" });
    }

    public void testComplexRegexRequiringDeterminizationSplit() throws IOException {
        Settings settings = Settings.builder().put("pattern", "(\\s+|,+)*").build();
        SimplePatternSplitTokenizerFactory factory = new SimplePatternSplitTokenizerFactory(
            IndexSettingsModule.newIndexSettings(new Index("test", "_na_"), Settings.EMPTY),
            null,
            "test",
            settings
        );

        Tokenizer tokenizer = factory.create();
        tokenizer.setReader(new StringReader("word1   word2,,,word3"));
        assertTokenStreamContents(tokenizer, new String[] { "word1", "word2", "word3" });
    }
}
