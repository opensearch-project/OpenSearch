/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.fielddata.ordinals;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOBooleanSupplier;
import org.apache.lucene.util.packed.PackedInts;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.index.fielddata.IndexOrdinalsFieldData;
import org.opensearch.index.fielddata.LeafOrdinalsFieldData;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.index.fielddata.plain.AbstractLeafOrdinalsFieldData;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Utility class to build global ordinals.
 *
 * @opensearch.internal
 */
public enum GlobalOrdinalsBuilder {
    ;

    /**
     * Returns {@code true} if {@code term} starts with the byte sequence {@code prefix}.
     *
     * <p>Shared by the scoped ordinal helpers ({@code PrefixScopedSortedSetDocValues} and
     * {@code NativeOrdPrefixTermsEnum}) so the prefix-matching logic lives in exactly one place.
     */
    static boolean startsWith(BytesRef term, BytesRef prefix) {
        if (term.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (term.bytes[term.offset + i] != prefix.bytes[prefix.offset + i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Build global ordinals for the provided {@link IndexReader}.
     */
    public static IndexOrdinalsFieldData build(
        final IndexReader indexReader,
        IndexOrdinalsFieldData indexFieldData,
        CircuitBreakerService breakerService,
        Logger logger,
        Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction
    ) throws IOException {
        return build(indexReader, indexFieldData, breakerService, logger, scriptFunction, () -> {});
    }

    /**
     * Build global ordinals for the provided {@link IndexReader}, with periodic cancellation checks
     * between segment iterations.
     */
    public static IndexOrdinalsFieldData build(
        final IndexReader indexReader,
        IndexOrdinalsFieldData indexFieldData,
        CircuitBreakerService breakerService,
        Logger logger,
        Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction,
        Runnable cancellationCheck
    ) throws IOException {
        assert indexReader.leaves().size() > 1;
        long startTimeNS = System.nanoTime();

        final LeafOrdinalsFieldData[] atomicFD = new LeafOrdinalsFieldData[indexReader.leaves().size()];
        final SortedSetDocValues[] subs = new SortedSetDocValues[indexReader.leaves().size()];
        // cancellableSubs wraps each segment's SortedSetDocValues with a cancellation-aware termsEnum()
        // for OrdinalMap.build(), which only calls termsEnum() and getValueCount().
        // atomicFD retains the original unwrapped values to preserve SingletonSortedSetDocValues
        // type for DocValues.unwrapSingleton().
        final SortedSetDocValues[] cancellableSubs = new SortedSetDocValues[indexReader.leaves().size()];
        for (int i = 0; i < indexReader.leaves().size(); ++i) {
            cancellationCheck.run();
            atomicFD[i] = indexFieldData.load(indexReader.leaves().get(i));
            final SortedSetDocValues ordinals = atomicFD[i].getOrdinalsValues();
            subs[i] = ordinals;
            cancellableSubs[i] = new CancellableTermsSortedSetDocValues(ordinals, cancellationCheck);
        }
        final OrdinalMap ordinalMap = OrdinalMap.build(null, cancellableSubs, PackedInts.DEFAULT);
        final long memorySizeInBytes = ordinalMap.ramBytesUsed();
        breakerService.getBreaker(CircuitBreaker.FIELDDATA).addEstimateBytesAndMaybeBreak(memorySizeInBytes, indexFieldData.getFieldName());

        if (logger.isDebugEnabled()) {
            logger.debug(
                "global-ordinals [{}][{}] took [{}]",
                indexFieldData.getFieldName(),
                ordinalMap.getValueCount(),
                new TimeValue(System.nanoTime() - startTimeNS, TimeUnit.NANOSECONDS)
            );
        }
        return new GlobalOrdinalsIndexFieldData(
            indexFieldData.getFieldName(),
            indexFieldData.getValuesSourceType(),
            atomicFD,
            ordinalMap,
            memorySizeInBytes,
            scriptFunction
        );
    }

    /**
     * Build a <b>group-scoped</b> global ordinals view for the provided {@link IndexReader}, restricted to the
     * join-field terms that start with {@code termPrefix}.
     * <p>
     * This is an optimisation for indices that store documents from many groups in the same index and whose
     * document (and join-key) ids are prefixed with a group id, e.g. {@code "<groupId>:<localId>"}. Because the
     * join-field term dictionary is sorted, all of a group's terms form a contiguous range, so passing
     * {@code termPrefix = "<groupId>:"} means {@link OrdinalMap#build} only merges that group's terms across
     * segments, making the build cost proportional to the number of that group's parent docs rather than the whole
     * shard. This avoids the multi-second {@code has_child} global-ordinals rewrite on large shards without the
     * eventual-consistency cost of {@code eager_global_ordinals}. It is only safe when queries are group-scoped
     * (already filtered to the same prefix).
     * <p>
     * Correctness note: each segment is exposed as a {@link SortedSetDocValues} whose terms enum iterates only the
     * in-prefix terms (a single group) <b>plus a sentinel at the segment's highest native ordinal</b>, while keeping
     * <b>native</b> ordinals and the full {@code getValueCount()} (see {@link PrefixScopedSortedSetDocValues} /
     * {@link NativeOrdPrefixTermsEnum}). Native ordinals are required because the join collector reads the segment's
     * raw doc-values and indexes the ordinal map by native ord; the sentinel makes {@link OrdinalMap} size each
     * segment's delta table over the full native range so out-of-prefix ords never throw AIOOBE. Only the group's
     * parents get a distinct global ord, so results are identical to the unscoped join.
     * <p>
     * This method builds a scoped map directly. Callers may retain it in the bounded scoped cache and must
     * release its breaker reservation when the map is evicted or a direct caller finishes using it.
     */
    public static IndexOrdinalsFieldData buildScoped(
        final IndexReader indexReader,
        IndexOrdinalsFieldData indexFieldData,
        CircuitBreakerService breakerService,
        Logger logger,
        Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction,
        BytesRef termPrefix,
        Runnable cancellationCheck
    ) throws IOException {
        assert indexReader.leaves().size() > 1;
        assert termPrefix != null;
        long startTimeNS = System.nanoTime();

        final LeafOrdinalsFieldData[] atomicFD = new LeafOrdinalsFieldData[indexReader.leaves().size()];
        final SortedSetDocValues[] scopedSubs = new SortedSetDocValues[indexReader.leaves().size()];
        for (int i = 0; i < indexReader.leaves().size(); ++i) {
            cancellationCheck.run();
            atomicFD[i] = indexFieldData.load(indexReader.leaves().get(i));
            // Bound the terms enum to [termPrefix, termPrefix+1) while preserving native segment ords.
            scopedSubs[i] = new PrefixScopedSortedSetDocValues(
                new CancellableTermsSortedSetDocValues(atomicFD[i].getOrdinalsValues(), cancellationCheck),
                termPrefix
            );
        }
        final OrdinalMap ordinalMap = OrdinalMap.build(null, scopedSubs, PackedInts.DEFAULT);
        final long memorySizeInBytes = ordinalMap.ramBytesUsed();
        final String fieldName = indexFieldData.getFieldName();
        final CircuitBreaker fieldDataBreaker = breakerService.getBreaker(CircuitBreaker.FIELDDATA);
        // Reserve map bytes and expose an idempotent release hook for cache eviction or direct callers.
        fieldDataBreaker.addEstimateBytesAndMaybeBreak(memorySizeInBytes, fieldName);
        final AtomicBoolean released = new AtomicBoolean(false);
        final Releasable breakerReleasable = () -> {
            if (released.compareAndSet(false, true)) {
                fieldDataBreaker.addWithoutBreaking(-memorySizeInBytes);
            }
        };

        if (logger.isDebugEnabled()) {
            logger.debug(
                "scoped-global-ordinals [{}][prefix={}][{}] took [{}]",
                fieldName,
                termPrefix.utf8ToString(),
                ordinalMap.getValueCount(),
                new TimeValue(System.nanoTime() - startTimeNS, TimeUnit.NANOSECONDS)
            );
        }
        return new GlobalOrdinalsIndexFieldData(
            fieldName,
            indexFieldData.getValuesSourceType(),
            atomicFD,
            ordinalMap,
            memorySizeInBytes,
            scriptFunction,
            breakerReleasable
        );
    }

    public static IndexOrdinalsFieldData buildEmpty(IndexReader indexReader, IndexOrdinalsFieldData indexFieldData) throws IOException {
        assert indexReader.leaves().size() > 1;

        final LeafOrdinalsFieldData[] atomicFD = new LeafOrdinalsFieldData[indexReader.leaves().size()];
        final SortedSetDocValues[] subs = new SortedSetDocValues[indexReader.leaves().size()];
        for (int i = 0; i < indexReader.leaves().size(); ++i) {
            atomicFD[i] = new AbstractLeafOrdinalsFieldData(AbstractLeafOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION) {
                @Override
                public SortedSetDocValues getOrdinalsValues() {
                    return DocValues.emptySortedSet();
                }

                @Override
                public long ramBytesUsed() {
                    return 0;
                }

                @Override
                public Collection<Accountable> getChildResources() {
                    return Collections.emptyList();
                }

                @Override
                public void close() {}
            };
            subs[i] = atomicFD[i].getOrdinalsValues();
        }
        final OrdinalMap ordinalMap = OrdinalMap.build(null, subs, PackedInts.DEFAULT);
        return new GlobalOrdinalsIndexFieldData(
            indexFieldData.getFieldName(),
            indexFieldData.getValuesSourceType(),
            atomicFD,
            ordinalMap,
            0,
            AbstractLeafOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION
        );
    }

    /**
     * Wraps a {@link SortedSetDocValues} to call cancellationCheck before advancing the terms enum.
     * Only cancellationCheck is invoked; the underlying doc-level iteration is not affected.
     * This preserves SingletonSortedSetDocValues type for unwrapSingleton() calls on atomicFD values.
     */
    private static class CancellableTermsSortedSetDocValues extends SortedSetDocValues {
        private final SortedSetDocValues in;
        private final Runnable cancellationCheck;

        CancellableTermsSortedSetDocValues(SortedSetDocValues in, Runnable cancellationCheck) {
            this.in = in;
            this.cancellationCheck = cancellationCheck;
        }

        @Override
        public TermsEnum termsEnum() throws IOException {
            TermsEnum te = in.termsEnum();
            return new FilterLeafReader.FilterTermsEnum(te) {
                private static final int CHECK_INTERVAL = (1 << 10) - 1; // 1023
                private int calls;

                @Override
                public BytesRef next() throws IOException {
                    if ((calls++ & CHECK_INTERVAL) == 0) {
                        cancellationCheck.run();
                    }
                    return in.next();
                }
            };
        }

        @Override
        public long getValueCount() {
            return in.getValueCount();
        }

        // Methods below are required by SortedSetDocValues but not called by OrdinalMap.build()
        @Override
        public int nextDoc() throws IOException {
            return in.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return in.advance(target);
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return in.advanceExact(target);
        }

        @Override
        public long nextOrd() throws IOException {
            return in.nextOrd();
        }

        @Override
        public int docValueCount() {
            return in.docValueCount();
        }

        @Override
        public BytesRef lookupOrd(long ord) throws IOException {
            return in.lookupOrd(ord);
        }

        @Override
        public int docID() {
            return in.docID();
        }

        @Override
        public long cost() {
            return in.cost();
        }
    }

    /**
     * A per-segment {@link SortedSetDocValues} view used only for building the scoped {@link OrdinalMap}.
     * Its {@link #termsEnum()} restricts iteration to the terms whose bytes start with {@code prefix}
     * (plus a sentinel), while all ordinals and {@link #getValueCount()} remain <b>native</b>.
     *
     * <p>Native ordinals are required for correctness: at collect time the join reads the segment's raw
     * {@code SortedDocValues} and indexes the ordinal map by native segment ord, so the scoped map must be
     * built in the same native space. The cost saving comes purely from {@link NativeOrdPrefixTermsEnum}
     * iterating only the group's terms (the {@link OrdinalMap#build} bottleneck), not from re-basing.
     */
    private static class PrefixScopedSortedSetDocValues extends SortedSetDocValues {
        private final SortedSetDocValues in;
        private final BytesRef prefix;
        private final long baseOrd;      // native ord of the first term >= prefix (0 if none)
        private final long scopedCount;  // number of terms in [prefix, prefix+1)

        PrefixScopedSortedSetDocValues(SortedSetDocValues in, BytesRef prefix) throws IOException {
            this.in = in;
            this.prefix = prefix;
            final long[] range = prefixOrdRange(in, prefix);
            this.baseOrd = range[0];
            this.scopedCount = range[1];
        }

        /** Returns {baseOrd, count} of the term range whose bytes start with {@code prefix}. */
        private static long[] prefixOrdRange(SortedSetDocValues values, BytesRef prefix) throws IOException {
            final long total = values.getValueCount();
            if (total == 0) {
                return new long[] { 0L, 0L };
            }
            final TermsEnum te = values.termsEnum();
            final TermsEnum.SeekStatus status = te.seekCeil(prefix);
            if (status == TermsEnum.SeekStatus.END) {
                return new long[] { 0L, 0L };
            }
            final long base = te.ord();
            long count = 0;
            BytesRef term = te.term();
            while (term != null && startsWith(term, prefix)) {
                count++;
                term = te.next();
            }
            return new long[] { base, count };
        }

        @Override
        public TermsEnum termsEnum() throws IOException {
            return new NativeOrdPrefixTermsEnum(in.termsEnum(), prefix, in.getValueCount());
        }

        @Override
        public long getValueCount() {
            // MUST be the full native value count: OrdinalMap sizes each segment's global-ord lookup to
            // this, and the join collector indexes it by the segment's NATIVE ordinal (docTermOrds.ordValue()).
            return in.getValueCount();
        }

        @Override
        public int nextDoc() throws IOException {
            return in.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return in.advance(target);
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return in.advanceExact(target);
        }

        @Override
        public long nextOrd() throws IOException {
            return in.nextOrd();
        }

        @Override
        public int docValueCount() {
            return in.docValueCount();
        }

        @Override
        public BytesRef lookupOrd(long ord) throws IOException {
            return in.lookupOrd(ord);
        }

        @Override
        public int docID() {
            return in.docID();
        }

        @Override
        public long cost() {
            return in.cost();
        }
    }

    /**
     * A {@link TermsEnum} that emits the terms starting with {@code prefix} (a single group's join keys),
     * plus a single trailing <b>sentinel</b> term positioned at the segment's <b>highest</b> native ordinal.
     * All ordinals reported are the segment's <b>native</b> ordinals.
     *
     * <p>Why native ords + a sentinel: the join collector reads the segment's raw {@code SortedDocValues}
     * and looks up global ords by <b>native</b> segment ordinal. {@link OrdinalMap} builds each segment's
     * global-ord delta table only up to the <b>highest ordinal it iterates</b>. Iterating just the prefix
     * range would size that table to the last in-prefix ord, so any out-of-prefix child on the shard (which
     * the collector still reads) would index past the table and throw AIOOBE. Emitting the last native ord
     * as a sentinel extends the table across the full native range, so every native lookup is in-bounds;
     * out-of-prefix ords resolve to a global ord that no in-prefix parent shares, so results stay correct.
     * This keeps {@link OrdinalMap#build}'s cost proportional to the group's terms (+O(1) sentinel) while
     * remaining safe for the full-range collector.
     */
    private static class NativeOrdPrefixTermsEnum extends TermsEnum {
        private final TermsEnum in;
        private final BytesRef prefix;
        private final long lastOrd;   // highest native ord in the segment (sentinel target), -1 if empty
        private boolean started = false;
        private boolean inPrefix = false;
        private boolean sentinelEmitted = false;
        private boolean exhausted = false;
        private long lastEmittedOrd = -1;   // highest native ord emitted so far (in-prefix terms)

        NativeOrdPrefixTermsEnum(TermsEnum in, BytesRef prefix, long fullValueCount) {
            this.in = in;
            this.prefix = prefix;
            this.lastOrd = fullValueCount - 1;
        }

        @Override
        public BytesRef next() throws IOException {
            if (exhausted) {
                return null;
            }
            // Phase 1: iterate the in-prefix terms.
            if (inPrefix || started == false) {
                final BytesRef t;
                if (started == false) {
                    started = true;
                    SeekStatus status = in.seekCeil(prefix);
                    if (status == SeekStatus.END) {
                        // No in-prefix terms; go straight to the sentinel (if any).
                        return emitSentinel();
                    }
                    t = in.term();
                } else {
                    t = in.next();
                }
                if (t != null && startsWith(t, prefix)) {
                    inPrefix = true;
                    lastEmittedOrd = in.ord();
                    return t;
                }
                // Left the prefix range; fall through to the sentinel.
                inPrefix = false;
                return emitSentinel();
            }
            // Phase 2: sentinel already handled below.
            return emitSentinel();
        }

        /**
         * Emits the segment's last native ord once, so OrdinalMap sizes the segment's global-ord table over
         * the full native range. Returns null when there is nothing (further) to emit.
         */
        private BytesRef emitSentinel() throws IOException {
            // Skip the sentinel when there is nothing to emit, or when the last in-prefix term already IS the
            // segment's highest native ord: re-emitting it would produce a duplicate ordinal and violate
            // OrdinalMap.build()'s strictly-increasing-ordinal contract (this happens when the group owns the
            // lexicographically last join term in the segment).
            if (sentinelEmitted || lastOrd < 0 || lastEmittedOrd == lastOrd) {
                exhausted = true;
                sentinelEmitted = true;
                return null;
            }
            sentinelEmitted = true;
            in.seekExact(lastOrd);
            return in.term();
        }

        @Override
        public long ord() throws IOException {
            // Native segment ordinal (both for in-prefix terms and the sentinel).
            return in.ord();
        }

        @Override
        public BytesRef term() throws IOException {
            return in.term();
        }

        @Override
        public int docFreq() throws IOException {
            return in.docFreq();
        }

        @Override
        public long totalTermFreq() throws IOException {
            return in.totalTermFreq();
        }

        @Override
        public SeekStatus seekCeil(BytesRef text) throws IOException {
            return in.seekCeil(text);
        }

        @Override
        public void seekExact(long ord) throws IOException {
            // Ordinals are native in this enum.
            in.seekExact(ord);
        }

        @Override
        public boolean seekExact(BytesRef text) throws IOException {
            return in.seekExact(text);
        }

        @Override
        public IOBooleanSupplier prepareSeekExact(BytesRef text) throws IOException {
            return in.prepareSeekExact(text);
        }

        @Override
        public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
            return in.postings(reuse, flags);
        }

        @Override
        public ImpactsEnum impacts(int flags) throws IOException {
            return in.impacts(flags);
        }

        @Override
        public TermState termState() throws IOException {
            return in.termState();
        }

        @Override
        public void seekExact(BytesRef term, TermState state) throws IOException {
            in.seekExact(term, state);
        }

        @Override
        public AttributeSource attributes() {
            return in.attributes();
        }
    }

}
