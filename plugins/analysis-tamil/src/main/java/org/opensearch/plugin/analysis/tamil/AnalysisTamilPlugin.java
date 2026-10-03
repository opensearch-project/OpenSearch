/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.analysis.tamil;

import org.apache.lucene.analysis.Analyzer;
import org.opensearch.index.analysis.AnalyzerProvider;
import org.opensearch.index.analysis.TokenFilterFactory;
import org.opensearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.opensearch.plugins.AnalysisPlugin;
import org.opensearch.plugins.Plugin;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;

/**
 * Plugin providing Tamil language analysis components for OpenSearch.
 * <p>
 * Registers:
 * <ul>
 *   <li>{@code tamil_stemmer} - A suffix-stripping token filter for Tamil</li>
 *   <li>{@code tamil_stop} - A stopword filter with a bundled Tamil stopword set</li>
 *   <li>{@code tamil} - A prebuilt analyzer combining the above filters</li>
 * </ul>
 */
public class AnalysisTamilPlugin extends Plugin implements AnalysisPlugin {

    /**
     * Creates a new AnalysisTamilPlugin.
     */
    public AnalysisTamilPlugin() {}

    @Override
    public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        Map<String, AnalysisProvider<TokenFilterFactory>> filters = new HashMap<>();
        filters.put("tamil_stemmer", TamilStemTokenFilterFactory::new);
        filters.put("tamil_stop", TamilStopTokenFilterFactory::new);
        return filters;
    }

    @Override
    public Map<String, AnalysisProvider<AnalyzerProvider<? extends Analyzer>>> getAnalyzers() {
        return singletonMap("tamil", TamilAnalyzerProvider::new);
    }
}
