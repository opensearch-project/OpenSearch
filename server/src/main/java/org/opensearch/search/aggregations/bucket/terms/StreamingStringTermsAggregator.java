/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

/**
 * Stream search terms aggregation
 */
public class StreamingStringTermsAggregator extends GlobalOrdinalsStringTermsAggregator {
    private SortedSetDocValues sortedDocValuesPerBatch;
    private long valueCount;

    public StreamingStringTermsAggregator(
        String name,
        AggregatorFactories factories,
        Function<StreamingStringTermsAggregator, ResultStrategy<?, ?, ?>> resultStrategy,
        ValuesSource.Bytes.WithOrdinals valuesSource,
        BucketOrder order,
        DocValueFormat format,
        BucketCountThresholds bucketCountThresholds,
        IncludeExclude.OrdinalsFilter includeExclude,
        SearchContext context,
        Aggregator parent,
        boolean remapGlobalOrds,
        SubAggCollectionMode collectionMode,
        boolean showTermDocCountError,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(
            name,
            factories,
            (GlobalOrdinalsStringTermsAggregator agg) -> resultStrategy.apply((StreamingStringTermsAggregator) agg),
            valuesSource,
            order,
            format,
            bucketCountThresholds,
            includeExclude,
            context,
            parent,
            remapGlobalOrds,
            collectionMode,
            showTermDocCountError,
            cardinality,
            metadata
        );
    }

    @Override
    public void doReset() {
        super.doReset();
        valueCount = 0;
        sortedDocValuesPerBatch = null;
        collectionStrategy.reset();
    }

    @Override
    protected boolean tryPrecomputeAggregationForLeaf(LeafReaderContext ctx) throws IOException {
        return false;
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return resultStrategy.buildAggregationsBatch(owningBucketOrds);
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        this.sortedDocValuesPerBatch = valuesSource.ordinalsValues(ctx);
        this.valueCount = sortedDocValuesPerBatch.getValueCount();
        this.docCounts = context.bigArrays().grow(docCounts, valueCount);

        SortedDocValues singleValues = DocValues.unwrapSingleton(sortedDocValuesPerBatch);
        if (singleValues != null) {
            segmentsWithSingleValuedOrds++;
            return resultStrategy.wrapCollector(new LeafBucketCollectorBase(sub, sortedDocValuesPerBatch) {
                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    if (false == singleValues.advanceExact(doc)) {
                        return;
                    }
                    int batchOrd = singleValues.ordValue();
                    collectionStrategy.collectGlobalOrd(owningBucketOrd, doc, batchOrd, sub);
                }
            });
        }
        segmentsWithMultiValuedOrds++;
        return resultStrategy.wrapCollector(new LeafBucketCollectorBase(sub, sortedDocValuesPerBatch) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (false == sortedDocValuesPerBatch.advanceExact(doc)) {
                    return;
                }
                int count = sortedDocValuesPerBatch.docValueCount();
                long globalOrd;
                while ((count-- > 0) && (globalOrd = sortedDocValuesPerBatch.nextOrd()) != SortedSetDocValues.NO_MORE_DOCS) {
                    collectionStrategy.collectGlobalOrd(owningBucketOrd, doc, globalOrd, sub);
                }
            }
        });
    }

    class StandardTermsResults extends GlobalOrdinalsStringTermsAggregator.StandardTermsResults {
        @Override
        StringTerms.Bucket buildFinalBucket(long globalOrd, long bucketOrd, long docCount, long owningBucketOrd) throws IOException {
            // Recreate DocValues as needed for concurrent segment search
            SortedSetDocValues values = getDocValues();
            BytesRef term = BytesRef.deepCopyOf(values.lookupOrd(globalOrd));

            StringTerms.Bucket result = new StringTerms.Bucket(term, docCount, null, showTermDocCountError, 0, format);
            result.bucketOrd = bucketOrd;
            result.docCountError = 0;
            return result;
        }
    }

    @Override
    SortedSetDocValues getDocValues() {
        return sortedDocValuesPerBatch;
    }
}
