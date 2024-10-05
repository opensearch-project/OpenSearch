/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analysis.phone;

import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AbstractIndexAnalyzerProvider;

/**
 * Provider for {@link PhoneNumberAnalyzer}.
 */
public class PhoneNumberAnalyzerProvider extends AbstractIndexAnalyzerProvider<PhoneNumberAnalyzer> {

    private final PhoneNumberAnalyzer analyzer;

    /**
     * @param indexSettings the settings of the index.
     * @param name the analyzer name.
     * @param settings the settings for the analyzer.
     * @param addNgrams defines whether ngrams for the phone number should be added. Set to true for indexing and false for search.
     */
    public PhoneNumberAnalyzerProvider(
        final IndexSettings indexSettings,
        final String name,
        final Settings settings,
        final boolean addNgrams
    ) {
        super(indexSettings, name, settings);
        this.analyzer = new PhoneNumberAnalyzer(settings, addNgrams);
    }

    @Override
    public PhoneNumberAnalyzer get() {
        return this.analyzer;
    }
}
