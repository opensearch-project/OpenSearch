package org.opensearch.analysis.layered;

import org.opensearch.index.analysis.TokenFilterFactory;
import org.opensearch.plugins.AnalysisPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.indices.analysis.AnalysisModule;

import java.util.Map;
import java.util.TreeMap;

/**
 * Registers the layered token filter using OpenSearch 2.19.0 SPI.
 */
public class LayeredAnalysisPlugin extends Plugin implements AnalysisPlugin {
    /**
     * Default constructor.
     */
    public LayeredAnalysisPlugin() {
	super();
    }       

    /**
     * getTokenFilters
     */
    @Override
    public Map<String, AnalysisModule.AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        Map<String, AnalysisModule.AnalysisProvider<TokenFilterFactory>> filters = new TreeMap<>();
        filters.put("layered", (indexSettings, environment, name, settings) -> new LayeredTokenFilterFactory());
        return filters;
    }
}
