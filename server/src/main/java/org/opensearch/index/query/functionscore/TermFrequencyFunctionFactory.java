/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query.functionscore;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.valuesource.SumTotalTermFreqValueSource;
import org.apache.lucene.queries.function.valuesource.TFValueSource;
import org.apache.lucene.queries.function.valuesource.TermFreqValueSource;
import org.apache.lucene.queries.function.valuesource.TotalTermFreqValueSource;
import org.apache.lucene.search.IndexSearcher;
import org.opensearch.common.lucene.BytesRefs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A factory class for creating instances of {@link TermFrequencyFunction}.
 * This class provides methods for creating different term frequency functions based on
 * the specified function name, field, and term. Each term frequency function is designed
 * to compute document scores based on specific term frequency calculations.
 *
 * @opensearch.internal
 */
public class TermFrequencyFunctionFactory {
    public static TermFrequencyFunction createFunction(
        TermFrequencyFunctionName functionName,
        String field,
        String term,
        LeafReaderContext readerContext,
        IndexSearcher indexSearcher
    ) throws IOException {
        switch (functionName) {
            case TERM_FREQ:
                TermFreqValueSource termFreqValueSource = new TermFreqValueSource(field, term, field, BytesRefs.toBytesRef(term));
                FunctionValues functionValues = termFreqValueSource.getValues(null, readerContext);
                return docId -> functionValues.intVal(docId);
            case TF:
                TFValueSource tfValueSource = new TFValueSource(field, term, field, BytesRefs.toBytesRef(term));
                Map<Object, Object> tfContext = new HashMap<>() {
                    {
                        put("searcher", indexSearcher);
                    }
                };
                functionValues = tfValueSource.getValues(tfContext, readerContext);
                return docId -> functionValues.floatVal(docId);
            case TOTAL_TERM_FREQ:
                TotalTermFreqValueSource totalTermFreqValueSource = new TotalTermFreqValueSource(
                    field,
                    term,
                    field,
                    BytesRefs.toBytesRef(term)
                );
                Map<Object, Object> ttfContext = new HashMap<>();
                totalTermFreqValueSource.createWeight(ttfContext, indexSearcher);
                functionValues = totalTermFreqValueSource.getValues(ttfContext, readerContext);
                return docId -> functionValues.longVal(docId);
            case SUM_TOTAL_TERM_FREQ:
                SumTotalTermFreqValueSource sumTotalTermFreqValueSource = new SumTotalTermFreqValueSource(field);
                Map<Object, Object> sttfContext = new HashMap<>();
                sumTotalTermFreqValueSource.createWeight(sttfContext, indexSearcher);
                functionValues = sumTotalTermFreqValueSource.getValues(sttfContext, readerContext);
                return docId -> functionValues.longVal(docId);
            default:
                throw new IllegalArgumentException("Unsupported function: " + functionName);
        }
    }

    /**
     * An enumeration representing the names of supported term frequency functions.
     */
    public enum TermFrequencyFunctionName {
        TERM_FREQ("termFreq"),
        TF("tf"),
        TOTAL_TERM_FREQ("totalTermFreq"),
        SUM_TOTAL_TERM_FREQ("sumTotalTermFreq");

        private final String termFrequencyFunctionName;

        TermFrequencyFunctionName(String termFrequencyFunctionName) {
            this.termFrequencyFunctionName = termFrequencyFunctionName;
        }

        public String getTermFrequencyFunctionName() {
            return termFrequencyFunctionName;
        }
    }
}
