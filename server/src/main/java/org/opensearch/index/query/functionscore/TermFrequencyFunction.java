/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query.functionscore;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.valuesource.SumTotalTermFreqValueSource;
import org.apache.lucene.queries.function.valuesource.TFValueSource;
import org.apache.lucene.queries.function.valuesource.TermFreqValueSource;
import org.apache.lucene.queries.function.valuesource.TotalTermFreqValueSource;
import org.apache.lucene.search.IndexSearcher;
import org.opensearch.common.lucene.BytesRefs;

import java.io.IOException;
import java.util.Map;

/**
 * Abstract class representing a term frequency function.
 */
public abstract class TermFrequencyFunction {

    protected final String field;
    protected final String term;
    protected final int docId;
    protected Map<Object, Object> context;

    public TermFrequencyFunction(String field, String term, int docId, Map<Object, Object> context) {
        this.field = field;
        this.term = term;
        this.docId = docId;
        this.context = context;
    }

    public abstract Object execute(LeafReaderContext readerContext) throws IOException;

    /**
     * Factory class to create term frequency functions.
     */
    public static class TermFrequencyFunctionFactory {
        public static TermFrequencyFunction createFunction(
            TermFrequencyFunctionNamesEnum functionName,
            String field,
            String term,
            int docId,
            Map<Object, Object> context
        ) {
            switch (functionName) {
                case TERM_FREQ:
                    return new TermFreqFunction(field, term, docId, context);
                case TF:
                    return new TFFunction(field, term, docId, context);
                case TOTAL_TERM_FREQ:
                    return new TotalTermFreq(field, term, docId, context);
                case SUM_TOTAL_TERM_FREQ:
                    return new SumTotalTermFreq(field, term, docId, context);
                default:
                    throw new IllegalArgumentException("Unsupported function: " + functionName);
            }
        }
    }

    /**
     * TermFreqFunction computes the term frequency in a field.
     */
    public static class TermFreqFunction extends TermFrequencyFunction {

        public TermFreqFunction(String field, String term, int docId, Map<Object, Object> context) {
            super(field, term, docId, context);
        }

        @Override
        public Integer execute(LeafReaderContext readerContext) throws IOException {
            TermFreqValueSource valueSource = new TermFreqValueSource(field, term, field, BytesRefs.toBytesRef(term));
            return valueSource.getValues(null, readerContext).intVal(docId);
        }
    }

    /**
     * TFFunction computes the term frequency-inverse document frequency (tf-idf) in a field.
     */
    public static class TFFunction extends TermFrequencyFunction {

        public TFFunction(String field, String term, int docId, Map<Object, Object> context) {
            super(field, term, docId, context);
        }

        @Override
        public Float execute(LeafReaderContext readerContext) throws IOException {
            TFValueSource valueSource = new TFValueSource(field, term, field, BytesRefs.toBytesRef(term));
            return valueSource.getValues(context, readerContext).floatVal(docId);
        }
    }

    /**
     * TotalTermFreq computes the total term frequency in a field.
     */
    public static class TotalTermFreq extends TermFrequencyFunction {

        public TotalTermFreq(String field, String term, int docId, Map<Object, Object> context) {
            super(field, term, docId, context);
        }

        @Override
        public Long execute(LeafReaderContext readerContext) throws IOException {
            TotalTermFreqValueSource valueSource = new TotalTermFreqValueSource(field, term, field, BytesRefs.toBytesRef(term));
            valueSource.createWeight(context, (IndexSearcher) context.get("searcher"));
            return valueSource.getValues(context, readerContext).longVal(docId);
        }
    }

    /**
     * SumTotalTermFreq computes the sum of total term frequencies within a field.
     */
    public static class SumTotalTermFreq extends TermFrequencyFunction {

        public SumTotalTermFreq(String field, String term, int docId, Map<Object, Object> context) {
            super(field, term, docId, context);
        }

        @Override
        public Long execute(LeafReaderContext readerContext) throws IOException {
            SumTotalTermFreqValueSource valueSource = new SumTotalTermFreqValueSource(field);
            valueSource.createWeight(context, (IndexSearcher) context.get("searcher"));
            return valueSource.getValues(context, readerContext).longVal(docId);
        }
    }

    /**
     * Enum representing the names of term frequency functions.
     */
    public enum TermFrequencyFunctionNamesEnum {
        TERM_FREQ("termFreq"),
        TF("tf"),
        TOTAL_TERM_FREQ("totalTermFreq"),
        SUM_TOTAL_TERM_FREQ("sumTotalTermFreq");

        private final String termFrequencyFunctionName;

        private TermFrequencyFunctionNamesEnum(String termFrequencyFunctionName) {
            this.termFrequencyFunctionName = termFrequencyFunctionName;
        }

        public String getTermFrequencyFunctionName() {
            return termFrequencyFunctionName;
        }
    }
}
