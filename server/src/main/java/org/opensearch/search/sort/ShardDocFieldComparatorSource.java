/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sort;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.SortField;
import org.opensearch.common.util.BigArrays;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.sort.BucketedSort.ExtraData;

/**
 * A pseudo‑field (_shard_doc) comparator that tiebreaks by  {@code (shardOrd << 32) | globalDocId}
 */
public class ShardDocFieldComparatorSource extends IndexFieldData.XFieldComparatorSource {
    public static final String NAME = "_shard_doc";

    private final int shardId;

    /**
     * @param shardId the ordinal of this shard within the coordinating node’s shard list
     */
    public ShardDocFieldComparatorSource(int shardId) {
        super(null, MultiValueMode.MIN, null);
        this.shardId = shardId;
    }

    @Override
    public SortField.Type reducedType() {
        return SortField.Type.LONG;
    }

    @Override
    public BucketedSort newBucketedSort(BigArrays bigArrays, SortOrder sortOrder, DocValueFormat format, int bucketSize, ExtraData extra) {
        throw new UnsupportedOperationException("bucketed sort not supported for " + NAME);
    }

    @Override
    public FieldComparator<Long> newComparator(String fieldname, int numHits, Pruning pruning, boolean reversed) {
        return new FieldComparator<Long>() {
            private final long[] values = new long[numHits];
            private long bottom;
            private long topValue;

            @Override
            public LeafFieldComparator getLeafComparator(LeafReaderContext context) {
                // derive a stable shard ordinal per-segment
                long shardOrd = shardId;
                final int docBase = context.docBase;

                return new LeafFieldComparator() {
                    Scorable scorer;

                    @Override
                    public void setScorer(Scorable scorer) {
                        this.scorer = scorer;
                    }

                    @Override
                    public void setBottom(int slot) {
                        bottom = values[slot];
                    }

                    @Override
                    public int compareBottom(int doc) {
                        long key = ((long) shardId << 32) | (docBase + doc);
                        return Long.compare(bottom, key);
                    }

                    @Override
                    public void copy(int slot, int doc) {
                        long key = ((long) shardId << 32) | (docBase + doc);
                        values[slot] = key;
                    }

                    @Override
                    public int compareTop(int doc) {
                        long key = ((long) shardId << 32) | (docBase + doc);
                        return Long.compare(topValue, key);
                    }
                };
            }

            @Override
            public int compare(int slot1, int slot2) {
                return Long.compare(values[slot1], values[slot2]);
            }

            @Override
            public Long value(int slot) {
                return values[slot];
            }

            @Override
            public void setTopValue(Long value) {
                this.topValue = value;
            }
        };
    }
}
