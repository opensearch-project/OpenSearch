/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.common.Nullable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BitArray;
import org.opensearch.search.aggregations.metrics.CardinalityAggregator.BucketOrdinalBitmaps;
import org.opensearch.search.aggregations.metrics.CardinalityAggregator.Collector;
import org.opensearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.BiConsumer;

/**
 * Collects the global ordinals of each bucket for the whole shard instead of hashing segment ordinals into the aggregator's sketches, so
 * that the parent ranks its buckets by their exact number of distinct values and only the buckets it keeps are hashed.
 * Global ordinals are the same in every segment, so each bucket's set keeps growing across segments.
 * <p>
 * Only with {@code execution_hint: deferred_ordinals}, as it only pays off when the parent orders its buckets by this
 * aggregation and has many more buckets than it keeps, which can't be told in advance: otherwise collecting global ordinals,
 * and building them if they aren't loaded, costs more than it saves. It also needs compact sketches, a field with ordinals
 * and global ordinals that fit in an {@code int}. It is decided on the first segment with values.
 * <p>
 * The ordinals go through the same stages as the hybrid collector, but they are kept rather than hashed when moving on: a
 * {@link BucketOrdinalBits} while it fits in the memory threshold, or a share of it for sparse ordinals, then
 * {@link BucketOrdinalBitmaps}. Once the bitmaps exceed the memory threshold, everything they hold is hashed into
 * the aggregator's sketches and collection carries on, in that segment and the following ones, as it would have without deferring.
 */
final class DeferredOrdinals implements Releasable {
    private static final Logger logger = LogManager.getLogger(DeferredOrdinals.class);

    private enum State {
        NOT_STARTED,
        NOT_DEFERRED,
        DEFERRING,
        STOPPED
    }

    private final CardinalityAggregator aggregator;
    private State state = State.NOT_STARTED;
    /** Why collection isn't deferred, for the profile. */
    private String reason;
    private ValuesSource.Bytes.WithOrdinals source;
    /** Any segment of the shard, to look the global ordinals up from. */
    private LeafReaderContext lookupLeaf;
    private SortedSetDocValues lookup;
    private long memoryThreshold;
    /** The ordinals while deferring, {@code null} otherwise: bits until they don't fit, then bitmaps. */
    @Nullable
    private BucketOrdinalSet ordinals;
    private boolean usedBitmaps;
    /** Buckets that {@link #prepareBuild} hashed, sorted. */
    private long[] hashedBuckets = new long[0];
    private int segments;
    private int bucketsBuilt;

    DeferredOrdinals(CardinalityAggregator aggregator) {
        this.aggregator = aggregator;
    }

    boolean isDeferring() {
        assert (state == State.DEFERRING) == (ordinals != null);
        return state == State.DEFERRING;
    }

    /**
     * The collector of a segment while collection is deferred, {@code null} otherwise.
     */
    @Nullable
    Collector leafCollector(ValuesSource.Bytes.WithOrdinals source, LeafReaderContext ctx) throws IOException {
        if (state == State.NOT_STARTED) {
            start(source, ctx);
        }
        if (state != State.DEFERRING) {
            return null;
        }
        segments++;
        return new SegmentCollector(ctx);
    }

    private void start(ValuesSource.Bytes.WithOrdinals source, LeafReaderContext ctx) throws IOException {
        reason = aggregator.whyNotDeferred(source, ctx);
        if (reason != null) {
            state = State.NOT_DEFERRED;
            return;
        }
        this.source = source;
        this.lookupLeaf = ctx;
        this.memoryThreshold = aggregator.memoryThreshold();
        this.ordinals = new BucketOrdinalBits(
            aggregator.bigArrays(),
            source.globalOrdinalsValues(ctx).getValueCount(),
            memoryThreshold,
            true
        );
        state = State.DEFERRING;
    }

    private SortedSetDocValues lookup() throws IOException {
        if (lookup == null) {
            lookup = source.globalOrdinalsValues(lookupLeaf);
        }
        return lookup;
    }

    /**
     * Adds the global ordinals of {@code doc} to {@code bucketOrd}, and returns whether they still fit in the memory
     * threshold.
     */
    private boolean add(SortedSetDocValues globalOrds, int doc, long bucketOrd) throws IOException {
        if (globalOrds.advanceExact(doc) == false) {
            return true;
        }
        int count = globalOrds.docValueCount();
        long ord;
        if (ordinals instanceof BucketOrdinalBits bits) {
            final BitArray bucketBits = bits.bucket(bucketOrd);
            if (bucketBits != null) {
                while ((count-- > 0) && (ord = globalOrds.nextOrd()) != SortedSetDocValues.NO_MORE_DOCS) {
                    bits.addCounted(bucketOrd, bucketBits, ord);
                }
                return true;
            }
            if (bits.isFull()) {
                // Dense ordinals would be about as big as bitmaps, so copying them would only double the memory for a while
                return false;
            }
            switchToBitmaps(bits);
        }
        final BucketOrdinalBitmaps bitmaps = (BucketOrdinalBitmaps) ordinals;
        while ((count-- > 0) && (ord = globalOrds.nextOrd()) != SortedSetDocValues.NO_MORE_DOCS) {
            bitmaps.add(bucketOrd, (int) ord);
        }
        return bitmaps.memoryUsed() <= memoryThreshold;
    }

    private void switchToBitmaps(BucketOrdinalBits bits) {
        final BucketOrdinalBitmaps bitmaps = new BucketOrdinalBitmaps(aggregator.bigArrays(), aggregator::chargeRequestBreaker);
        boolean success = false;
        try {
            bits.copyTo(bitmaps);
            success = true;
        } finally {
            if (success == false) {
                bitmaps.close();
            }
        }
        ordinals = bitmaps;
        bits.close();
    }

    /**
     * Hashes the ordinals collected so far into the aggregator's sketches and goes back to collecting segment ordinals.
     */
    private void stop() throws IOException {
        final BucketOrdinalSet stopped = ordinals;
        ordinals = null;
        state = State.STOPPED;
        usedBitmaps = stopped instanceof BucketOrdinalBitmaps;
        final long bytes = stopped instanceof BucketOrdinalBits bits ? BucketOrdinalBits.collectIntoBytes(bits.bucketsWithBits()) : 0;
        aggregator.chargeRequestBreaker(bytes);
        try {
            stopped.collectInto(aggregator.counts, lookup(), null);
        } finally {
            aggregator.chargeRequestBreaker(-bytes);
            stopped.close();
        }
        logger.debug("Cardinality aggregation [{}] stopped deferring collection: global ordinals exceed the threshold", aggregator.name());
    }

    /**
     * Exact number of distinct values collected into {@code bucketOrd}.
     */
    long cardinality(long bucketOrd) {
        return ordinals.cardinality(bucketOrd);
    }

    /**
     * Hashes the buckets about to be built all at once, so that values they share are looked up and hashed once.
     */
    void prepareBuild(long[] owningBucketOrds) throws IOException {
        if (isDeferring() == false) {
            return;
        }
        // The buckets with values, sorted and distinct, kept until the aggregator is closed
        aggregator.chargeRequestBreaker(RamUsageEstimator.sizeOf(owningBucketOrds));
        long[] buckets = new long[owningBucketOrds.length];
        int n = 0;
        for (long bucket : owningBucketOrds) {
            if (cardinality(bucket) > 0) {
                buckets[n++] = bucket;
            }
        }
        Arrays.sort(buckets, 0, n);
        int distinct = 0;
        for (int i = 0; i < n; i++) {
            if (distinct == 0 || buckets[i] != buckets[distinct - 1]) {
                buckets[distinct++] = buckets[i];
            }
        }
        buckets = Arrays.copyOf(buckets, distinct);
        hashInto(buckets);
        bucketsBuilt += buckets.length;
        hashedBuckets = buckets;
    }

    private void hashInto(long[] buckets) throws IOException {
        final long bytes = ordinals instanceof BucketOrdinalBits ? BucketOrdinalBits.collectIntoBytes(buckets.length) : 0;
        aggregator.chargeRequestBreaker(bytes);
        try {
            ordinals.collectInto(aggregator.counts, lookup(), buckets);
        } finally {
            aggregator.chargeRequestBreaker(-bytes);
        }
    }

    /**
     * Hashes {@code bucketOrd} into the aggregator's sketches unless {@link #prepareBuild} did, and returns whether it holds values.
     */
    boolean build(long bucketOrd) throws IOException {
        if (cardinality(bucketOrd) == 0) {
            return false;
        }
        if (Arrays.binarySearch(hashedBuckets, bucketOrd) < 0) {
            bucketsBuilt++;
            hashInto(new long[] { bucketOrd });
        }
        return true;
    }

    void collectDebugInfo(BiConsumer<String, Object> add) {
        add.accept("deferred_ordinals", switch (state) {
            case NOT_STARTED -> "not_started";
            case NOT_DEFERRED -> reason;
            case DEFERRING -> "used";
            case STOPPED -> "stopped_over_memory_threshold";
        });
        add.accept("deferred_bitmaps", ordinals instanceof BucketOrdinalBitmaps || usedBitmaps);
        add.accept("deferred_segments", segments);
        add.accept("deferred_buckets_built", bucketsBuilt);
    }

    @Override
    public void close() {
        Releasables.close(ordinals);
    }

    /**
     * Collects a segment's global ordinals until they exceed the memory threshold, then the rest of the segment with the
     * collector that would have been used without deferring. The document that went over the threshold is collected again,
     * which doesn't change the sketches.
     */
    private final class SegmentCollector extends Collector {
        private final LeafReaderContext ctx;
        private final SortedSetDocValues globalOrds;
        @Nullable
        private Collector fallback;

        SegmentCollector(LeafReaderContext ctx) throws IOException {
            this.ctx = ctx;
            this.globalOrds = source.globalOrdinalsValues(ctx);
        }

        @Override
        public void collect(int doc, long owningBucketOrd) throws IOException {
            if (fallback != null) {
                fallback.collect(doc, owningBucketOrd);
            } else if (add(globalOrds, doc, owningBucketOrd) == false) {
                stop();
                fallback = aggregator.ordinalsCollector(source, source.ordinalsValues(ctx), ctx);
                fallback.collect(doc, owningBucketOrd);
            }
        }

        @Override
        public void postCollect() throws IOException {
            if (fallback != null) {
                fallback.postCollect();
            } else if (ordinals instanceof BucketOrdinalBitmaps bitmaps) {
                bitmaps.flushBreaker();
            }
        }

        @Override
        public void close() {
            Releasables.close(fallback);
        }
    }
}
