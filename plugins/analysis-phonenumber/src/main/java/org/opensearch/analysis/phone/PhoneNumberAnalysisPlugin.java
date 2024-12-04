/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analysis.phone;

import org.apache.lucene.analysis.Analyzer;
import org.opensearch.index.analysis.AnalyzerProvider;
import org.opensearch.index.analysis.TokenizerFactory;
import org.opensearch.indices.analysis.AnalysisModule;
import org.opensearch.plugins.AnalysisPlugin;
import org.opensearch.plugins.Plugin;

import java.util.Map;
import java.util.TreeMap;

/**
 * This plugin provides an analyzer and tokenizer for fields which contain phone numbers, supporting a variety of formats
 * (with/without international calling code, different country formats, etc.).
 */
public class PhoneNumberAnalysisPlugin extends Plugin implements AnalysisPlugin {

    @Override
    public Map<String, AnalysisModule.AnalysisProvider<AnalyzerProvider<? extends Analyzer>>> getAnalyzers() {
        Map<String, AnalysisModule.AnalysisProvider<AnalyzerProvider<? extends Analyzer>>> analyzers = new TreeMap<>();
        analyzers.put(
            "phone",
            (indexSettings, environment, name, settings) -> new PhoneNumberAnalyzerProvider(indexSettings, "phone", settings, true)
        );
        analyzers.put(
            "phone-search",
            (indexSettings, environment, name, settings) -> new PhoneNumberAnalyzerProvider(indexSettings, "phone-search", settings, false)
        );
        return analyzers;
    }

    @Override
    public Map<String, AnalysisModule.AnalysisProvider<TokenizerFactory>> getTokenizers() {
        Map<String, AnalysisModule.AnalysisProvider<TokenizerFactory>> tokenizers = new TreeMap<>();
        tokenizers.put(
            "phone",
            (indexSettings, environment, name, settings) -> new PhoneNumberTermTokenizerFactory(indexSettings, "phone", settings, true)
        );
        tokenizers.put(
            "phone-search",
            (indexSettings, environment, name, settings) -> new PhoneNumberTermTokenizerFactory(
                indexSettings,
                "phone-search",
                settings,
                false
            )
        );
        return tokenizers;
    }

}
