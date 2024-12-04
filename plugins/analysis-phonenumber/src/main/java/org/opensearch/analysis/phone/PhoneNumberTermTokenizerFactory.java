/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analysis.phone;

import org.apache.lucene.analysis.Tokenizer;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AbstractTokenizerFactory;

/**
 * Factory for {@link PhoneNumberTermTokenizer}.
 */
public class PhoneNumberTermTokenizerFactory extends AbstractTokenizerFactory {
    private final Settings settings;
    private final boolean addNgrams;

    /**
     * @param indexSettings the settings of the index.
     * @param name the tokenizer name.
     * @param settings the settings for the analyzer.
     * @param addNgrams defines whether ngrams for the phone number should be added. Set to true for indexing and false for search.
     */
    public PhoneNumberTermTokenizerFactory(
        final IndexSettings indexSettings,
        final String name,
        final Settings settings,
        final boolean addNgrams
    ) {
        super(indexSettings, settings, name);
        this.settings = settings;
        this.addNgrams = addNgrams;
    }

    @Override
    public Tokenizer create() {
        return new PhoneNumberTermTokenizer(this.settings, this.addNgrams);
    }
}
