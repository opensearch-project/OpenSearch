package org.opensearch.analysis.layered;

import org.apache.lucene.analysis.TokenStream;
import org.opensearch.index.analysis.TokenFilterFactory;

/**
 * Factory for the layered token filter.
 */
public class LayeredTokenFilterFactory implements TokenFilterFactory {

    /**
     * Default constructor.
     */
    public LayeredTokenFilterFactory() {}

    @Override
    public String name() {
        return "layered";
    }

    @Override
    public TokenStream create(TokenStream input) {
        return new LayeredTokenFilter(input);
    }
}
