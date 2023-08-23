/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.lookup;

import org.apache.lucene.search.IndexSearcher;
import org.opensearch.index.query.functionscore.TermFrequencyFunction;
import org.opensearch.index.query.functionscore.TermFrequencyFunctionFactory;
import org.opensearch.index.query.functionscore.TermFrequencyFunctionFactory.TermFrequencyFunctionName;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Looks up term frequency per-segment
 *
 * @opensearch.internal
 */
public class LeafTermFrequencyLookup {

    private final IndexSearcher indexSearcher;
    private final LeafSearchLookup leafLookup;
    private final Map<String, TermFrequencyFunction> termFreqCache;

    public LeafTermFrequencyLookup(IndexSearcher indexSearcher, LeafSearchLookup leafLookup) {
        this.indexSearcher = indexSearcher;
        this.leafLookup = leafLookup;
        this.termFreqCache = new HashMap<>();
    }

    public Object getTermFrequency(TermFrequencyFunctionName functionName, String field, String val, int docId) throws IOException {
        TermFrequencyFunction termFrequencyFunction = getOrCreateTermFrequencyFunction(functionName, field, val);
        return termFrequencyFunction.execute(docId);
    }

    private TermFrequencyFunction getOrCreateTermFrequencyFunction(TermFrequencyFunctionName functionName, String field, String val)
        throws IOException {
        String cacheKey = (val == null)
            ? String.format(Locale.ROOT, "%s-%s", functionName, field)
            : String.format(Locale.ROOT, "%s-%s-%s", functionName, field, val);

        if (!termFreqCache.containsKey(cacheKey)) {
            TermFrequencyFunction termFrequencyFunction = TermFrequencyFunctionFactory.createFunction(
                functionName,
                field,
                val,
                leafLookup.ctx,
                indexSearcher
            );
            termFreqCache.put(cacheKey, termFrequencyFunction);
        }

        return termFreqCache.get(cacheKey);
    }
}
