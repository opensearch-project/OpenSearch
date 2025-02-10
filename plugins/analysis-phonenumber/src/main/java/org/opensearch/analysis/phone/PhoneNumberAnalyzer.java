/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analysis.phone;

import org.apache.lucene.analysis.Analyzer;
import org.opensearch.common.settings.Settings;

/**
 * Analyzer for phone numbers, using {@link PhoneNumberTermTokenizer}.
 *
 * <p>
 * You can use the {@code phone} and {@code phone-search} analyzers on your fields to index phone numbers.
 * Use {@code phone} (which creates ngrams) for the {@code analyzer} and {@code phone-search} (which doesn't create ngrams)
 * for the {@code search_analyzer}.
 * </p>
 *
 * <p>
 * You optionally can specify a region with the {@code phone-region} setting for the phone number which will ensure that
 * phone numbers without the international  dialling prefix (using {@code +}) are also tokenized correctly.
 * </p>
 *
 * <p>
 * Note that the tokens will not refer to a specific position in the stream as the tokenizer is expected to be used on strings
 * containing phone numbers and not arbitrary text with interspersed phone numbers.
 * </p>
 */
public class PhoneNumberAnalyzer extends Analyzer {
    private final boolean addNgrams;
    private final Settings settings;

    /**
     * @param addNgrams defines whether ngrams for the phone number should be added. Set to true for indexing and false for search.
     * @param settings the settings for the analyzer.
     */
    public PhoneNumberAnalyzer(final Settings settings, final boolean addNgrams) {
        this.addNgrams = addNgrams;
        this.settings = settings;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        final var tokenizer = new PhoneNumberTermTokenizer(this.settings, this.addNgrams);
        return new Analyzer.TokenStreamComponents(tokenizer, tokenizer);
    }
}
