/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.analytics.spi.DelegatedExpression;
import org.opensearch.analytics.spi.FilterDelegationHandle;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

/**
 * Lucene implementation of {@link FilterDelegationHandle}. Compiles delegated expressions
 * into Lucene Queries, creates Weights on demand, and produces bitsets via Scorers.
 *
 * <p>Segments are resolved by <b>writer generation</b>. The mapping
 * {@code generation → Lucene leaf index} is provided by {@link LuceneReader}, which is
 * built once at refresh time in {@link LuceneReaderManager}.
 *
 * @opensearch.internal
 */
final class LuceneFilterDelegationHandle implements FilterDelegationHandle {

    private static final Logger LOGGER = LogManager.getLogger(LuceneFilterDelegationHandle.class);

    // TODO: lazy query compilation for performance-delegated predicates. Today
    // every delegated expression is compiled (QueryBuilder → Lucene Query) at
    // ctor time. For correctness-delegated predicates (always called) this is
    // fine. For performance-delegated predicates that DF page-pruning may never
    // consult, the compile cost is wasted. Deferring needs a way to distinguish
    // the two kinds (e.g. add a kind field on DelegatedExpression) and clear
    // semantics for compile-failure timing (eager = fail at ctor, lazy = fail
    // at first use). Revisit if this surfaces as a real cost — needs revisiting.
    private final Map<Integer, Query> queriesByAnnotationId;
    private final DirectoryReader directoryReader;
    private final IndexSearcher searcher;
    private final List<LeafReaderContext> leaves;
    private final BooleanSupplier isCancelledSupplier;
    private final Map<Long, String> generationToSegmentName;

    private final ConcurrentHashMap<Integer, Weight> weightsByProviderKey = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, ScorerHandle> scorersByCollectorKey = new ConcurrentHashMap<>();
    /** Provider keys created from {@link #LIVE_DOCS_MATCH_ALL_ANNOTATION_ID} — their collectors emit liveDocs directly. */
    private final java.util.Set<Integer> liveDocsProviderKeys = ConcurrentHashMap.newKeySet();
    private final AtomicInteger nextProviderKey = new AtomicInteger(1);
    private final AtomicInteger nextCollectorKey = new AtomicInteger(1);

    LuceneFilterDelegationHandle(
        List<DelegatedExpression> expressions,
        QueryShardContext queryShardContext,
        LuceneReader luceneReader,
        CatalogSnapshot catalogSnapshot,
        NamedWriteableRegistry namedWriteableRegistry,
        BooleanSupplier isCancelledSupplier
    ) {
        assert luceneReader != null : "luceneReader must not be null";
        assert catalogSnapshot != null : "catalogSnapshot must not be null";
        this.directoryReader = luceneReader.directoryReader();
        // Use the shared per-reader searcher (LuceneReader#searcher). It is built over THIS
        // directoryReader — the same reader whose leaves we score against in createCollector
        // (weight.scorer(leaf)) — so the Weight's top-reader matches the scored leaf's top-reader and
        // the IndicesQueryCache wrapper's assertion holds (a searcher over a DIFFERENT reader, e.g. the
        // old queryShardContext.searcher(), threw the fatal-under-`-ea` "top-reader used to create
        // Weight is not the same as the current reader's top-reader" AssertionError). Reusing this
        // searcher (vs a fresh plain IndexSearcher, which has no query cache) keeps the node
        // IndicesQueryCache wired in, so repeated delegated predicates populate + hit the shard query
        // cache (QueryCacheIT). The shared instance was already built cache-enabled by the caller
        // (LuceneAnalyticsBackendPlugin#getFilterDelegationHandle passes the cache + policy); later
        // calls return that instance and ignore the args.
        this.searcher = luceneReader.searcher(null, null);
        this.leaves = directoryReader.leaves();
        this.generationToSegmentName = luceneReader.generationToSegmentName();
        this.queriesByAnnotationId = compileQueries(expressions, queryShardContext, namedWriteableRegistry);
        this.isCancelledSupplier = isCancelledSupplier;
    }

    private static Map<Integer, Query> compileQueries(
        List<DelegatedExpression> expressions,
        QueryShardContext context,
        NamedWriteableRegistry registry
    ) {
        Map<Integer, Query> queries = new HashMap<>();
        for (DelegatedExpression expr : expressions) {
            try {
                StreamInput rawInput = StreamInput.wrap(expr.getExpressionBytes());
                StreamInput input = new NamedWriteableAwareStreamInput(rawInput, registry);
                QueryBuilder queryBuilder = input.readNamedWriteable(QueryBuilder.class);
                // Rewrite FieldExistsQuery → a postings-only equivalent: the lucene-secondary segment
                // has no doc_values/norms (they live in the parquet primary), so a FieldExistsQuery
                // built from an _exists_ clause (PPL `search field!=value`) would throw at rewrite().
                Query query = LuceneQueryConversionUtils.rewriteFieldExistsForSecondary(queryBuilder.toQuery(context));
                queries.put(expr.getAnnotationId(), query);
            } catch (IOException exception) {
                throw new IllegalStateException(
                    "Failed to deserialize delegated expression for annotationId=" + expr.getAnnotationId(),
                    exception
                );
            }
        }
        // Deleted-doc filtering: always register the reserved match-all query so the driving
        // backend can AND a synthetic live-docs Collector into its filter tree when the shard has
        // deletions — including for pure-DF queries where `expressions` is empty. Its collector
        // short-circuits to the segment's liveDocs in collectDocs (see ScorerHandle#emitLiveDocs).
        // Registration is a single map entry; no Weight is created unless the id is actually used.
        queries.put(LIVE_DOCS_MATCH_ALL_ANNOTATION_ID, new MatchAllDocsQuery());
        return queries;
    }

    @Override
    public int createProvider(int annotationId) {
        Query query = queriesByAnnotationId.get(annotationId);
        if (query == null) {
            return -1;
        }
        try {
            Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
            int providerKey = nextProviderKey.getAndIncrement();
            weightsByProviderKey.put(providerKey, weight);
            if (annotationId == LIVE_DOCS_MATCH_ALL_ANNOTATION_ID) {
                // Collectors created from this provider emit the segment's liveDocs directly
                // (word-wise copy) instead of iterating a match-all scorer doc-by-doc.
                liveDocsProviderKeys.add(providerKey);
            }
            LOGGER.debug("[scf] createProvider annotationId={} → providerKey={}", annotationId, providerKey);
            return providerKey;
        } catch (IOException exception) {
            LOGGER.error("createProvider failed for annotationId=" + annotationId, exception);
            return -1;
        }
    }

    @Override
    public int createCollector(int providerKey, long writerGeneration, int minDoc, int maxDoc) {
        Weight weight = weightsByProviderKey.get(providerKey);
        if (weight == null) {
            return -1;
        }
        String segName = generationToSegmentName.get(writerGeneration);
        if (segName == null) {
            LOGGER.error(
                "createCollector: no Lucene segment for writer_generation={} (providerKey={}). Known generations: {}",
                writerGeneration,
                providerKey,
                generationToSegmentName.keySet()
            );
            return -1;
        }
        LeafReaderContext leaf = null;
        for (LeafReaderContext lrc : leaves) {
            if (unwrapSegmentReader(lrc.reader()).getSegmentInfo().info.name.equals(segName)) {
                leaf = lrc;
                break;
            }
        }
        if (leaf == null) {
            LOGGER.error(
                "createCollector: segment name [{}] not found in leaves (writerGeneration={}, providerKey={})",
                segName,
                writerGeneration,
                providerKey
            );
            return -1;
        }

        int leafMaxDoc = leaf.reader().maxDoc();
        assert minDoc >= 0 && minDoc <= maxDoc && maxDoc <= leafMaxDoc : "createCollector(providerKey="
            + providerKey
            + ", writerGeneration="
            + writerGeneration
            + " -> segment="
            + segName
            + "): partition ["
            + minDoc
            + ","
            + maxDoc
            + ") exceeds leaf maxDoc="
            + leafMaxDoc;

        try {
            // Segment live docs (null when the segment has no deletions). Captured per collector so
            // collectDocs can (a) drop deleted docs from ordinary scorer iteration, and (b) emit the
            // live set directly for the reserved match-all provider (deleted-doc filtering path).
            org.apache.lucene.util.Bits liveDocs = leaf.reader().getLiveDocs();
            boolean emitLiveDocs = liveDocsProviderKeys.contains(providerKey);
            // The match-all provider never iterates a scorer — its bitset is exactly the live docs
            // (all-ones when the segment has no deletions) — so skip scorer creation entirely.
            Scorer scorer = emitLiveDocs ? null : weight.scorer(leaf);
            int collectorKey = nextCollectorKey.getAndIncrement();
            scorersByCollectorKey.put(collectorKey, new ScorerHandle(scorer, minDoc, maxDoc, liveDocs, emitLiveDocs));
            LOGGER.debug(
                "[scf] createCollector providerKey={} writerGeneration={} range=[{},{}) → collectorKey={}",
                providerKey,
                writerGeneration,
                minDoc,
                maxDoc,
                collectorKey
            );
            return collectorKey;
        } catch (IOException exception) {
            LOGGER.error(
                "createCollector failed for providerKey=" + providerKey + ", writerGeneration=" + writerGeneration + ", segment=" + segName,
                exception
            );
            return -1;
        }
    }

    @Override
    public boolean isCancelled() {
        return isCancelledSupplier != null && isCancelledSupplier.getAsBoolean();
    }

    @Override
    public long collectDocs(int collectorKey, int minDoc, int maxDoc, MemorySegment out) {
        ScorerHandle handle = scorersByCollectorKey.get(collectorKey);
        if (handle == null) {
            return -1;
        }
        if (maxDoc <= minDoc) {
            return 0;
        }
        int span = maxDoc - minDoc;
        FixedBitSet bits = new FixedBitSet(span);
        int nextDoc = Integer.MAX_VALUE;

        if (handle.emitLiveDocs) {
            // Reserved match-all provider (deleted-doc filtering): the bitset is exactly the
            // segment's live docs over the requested range — no scorer iteration. nextDoc is
            // reported as maxDoc (match-all never exhausts), so callers never skip later RGs.
            int scanFrom = Math.max(minDoc, handle.partitionMinDoc);
            int scanTo = Math.min(maxDoc, handle.partitionMaxDoc);
            int wordCount = (span + 63) >>> 6;
            if (scanFrom < scanTo && handle.liveDocs != null && scanFrom == minDoc && scanTo == maxDoc) {
                // Common case (RG chunk fully inside the partition): word-wise copy of the
                // liveDocs slice straight into the out buffer (set bit == live).
                fillLiveDocsWords(handle.liveDocs, minDoc, span, wordCount, out);
                return ((long) maxDoc << 32) | (wordCount & 0xFFFFFFFFL);
            }
            if (scanFrom < scanTo) {
                if (handle.liveDocs == null) {
                    // Segment has no deletions — every doc in range is live.
                    bits.set(scanFrom - minDoc, scanTo - minDoc);
                } else {
                    for (int doc = scanFrom; doc < scanTo; doc++) {
                        if (handle.liveDocs.get(doc)) {
                            bits.set(doc - minDoc);
                        }
                    }
                }
            }
            nextDoc = maxDoc;
        } else if (handle.scorer != null) {
            int scanFrom = Math.max(minDoc, handle.partitionMinDoc);
            int scanTo = Math.min(maxDoc, handle.partitionMaxDoc);

            if (scanFrom < scanTo) {
                try {
                    DocIdSetIterator iterator = handle.scorer.iterator();
                    int docId = handle.currentDoc;
                    if (docId != DocIdSetIterator.NO_MORE_DOCS) {
                        if (docId < scanFrom) {
                            docId = iterator.advance(scanFrom);
                        }
                        while (docId != DocIdSetIterator.NO_MORE_DOCS && docId < scanTo) {
                            // Deleted-doc filtering: Weight.scorer iterators do NOT consult liveDocs
                            // (Lucene applies them as acceptDocs in BulkScorer, which this path
                            // bypasses), so drop deleted docs here. No-op on segments without
                            // deletions (liveDocs == null).
                            if (handle.liveDocs == null || handle.liveDocs.get(docId)) {
                                bits.set(docId - minDoc);
                            }
                            docId = iterator.nextDoc();
                        }
                        handle.currentDoc = docId;
                    }
                    nextDoc = handle.currentDoc;
                } catch (IOException exception) {
                    LOGGER.warn("IOException during collectDocs, returning partial bitset", exception);
                    // Iteration is only partial — don't signal exhaustion (MAX_VALUE),
                    // which would make callers skip all subsequent RGs for this leaf.
                    // Report maxDoc conservatively so later RGs are still probed.
                    nextDoc = maxDoc;
                }
            } else {
                nextDoc = handle.currentDoc;
            }
        }

        long[] words = bits.getBits();
        int wordCount = (span + 63) >>> 6;
        MemorySegment.copy(words, 0, out, ValueLayout.JAVA_LONG, 0, wordCount);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "[scf] collectDocs collectorKey={} range=[{},{}) → cardinality={} words={} nextDoc={}",
                collectorKey,
                minDoc,
                maxDoc,
                bits.cardinality(),
                wordCount,
                nextDoc
            );
        }
        return ((long) nextDoc << 32) | (wordCount & 0xFFFFFFFFL);
    }

    @Override
    public void releaseCollector(int collectorKey) {
        scorersByCollectorKey.remove(collectorKey);
    }

    @Override
    public void releaseProvider(int providerKey) {
        weightsByProviderKey.remove(providerKey);
        liveDocsProviderKeys.remove(providerKey);
    }

    /**
     * Pack the LIVE-docs slice {@code [minDoc, minDoc+span)} into {@code out} as {@code wordCount}
     * LSB-first longs (set bit == live). Used by the reserved match-all collector (deleted-doc
     * filtering path) in {@link #collectDocs}. Dense segments recover the backing {@link FixedBitSet}
     * (O(words)); sparse segments fill all-alive then clear the O(deletions) deleted bits; anything
     * else falls back to a per-bit loop. Caller guarantees {@code liveDocs != null} and {@code span > 0}.
     */
    private static void fillLiveDocsWords(org.apache.lucene.util.Bits liveDocs, int minDoc, int span, int wordCount, MemorySegment out) {
        int maxDoc = minDoc + span;
        if (liveDocs instanceof org.apache.lucene.util.LiveDocs ld) {
            FixedBitSet liveBits = org.apache.lucene.util.BitSetIterator.getFixedBitSetOrNull(ld.liveDocsIterator());
            if (liveBits != null) {
                copyLiveWords(liveBits, out, minDoc, span, wordCount);
                return;
            }
            for (int w = 0; w < wordCount; w++) {
                out.setAtIndex(ValueLayout.JAVA_LONG, w, -1L);
            }
            int trailingBits = span & 63;
            if (trailingBits != 0) {
                out.setAtIndex(ValueLayout.JAVA_LONG, wordCount - 1, (1L << trailingBits) - 1);
            }
            try {
                DocIdSetIterator deleted = ld.deletedDocsIterator();
                int doc = deleted.advance(minDoc);
                while (doc != DocIdSetIterator.NO_MORE_DOCS && doc < maxDoc) {
                    int rel = doc - minDoc;
                    int w = rel >>> 6;
                    long cur = out.getAtIndex(ValueLayout.JAVA_LONG, w);
                    out.setAtIndex(ValueLayout.JAVA_LONG, w, cur & ~(1L << (rel & 63)));
                    doc = deleted.nextDoc();
                }
                return;
            } catch (IOException e) {
                LOGGER.warn("[scf] fillLiveDocsWords deletedDocsIterator failed; falling back to per-bit", e);
            }
        }

        if (liveDocs instanceof FixedBitSet fbs) {
            copyLiveWords(fbs, out, minDoc, span, wordCount);
            return;
        }

        long word = 0;
        int wordIdx = 0;
        for (int i = 0; i < span; i++) {
            if (liveDocs.get(minDoc + i)) {
                word |= (1L << (i & 63));
            }
            if ((i & 63) == 63) {
                out.setAtIndex(ValueLayout.JAVA_LONG, wordIdx, word);
                word = 0;
                wordIdx++;
            }
        }
        if ((span & 63) != 0) {
            out.setAtIndex(ValueLayout.JAVA_LONG, wordIdx, word);
        }
    }

    /**
     * Copy the LIVE-docs slice {@code [effectiveMinDoc, effectiveMinDoc + span)} of a
     * {@link FixedBitSet} into {@code out} as {@code wordCount} packed longs (set bit == live).
     */
    private static void copyLiveWords(FixedBitSet fbs, MemorySegment out, int effectiveMinDoc, int span, int wordCount) {
        long[] srcWords = fbs.getBits();
        int startWord = effectiveMinDoc >>> 6;
        int bitOffset = effectiveMinDoc & 63;

        if (bitOffset == 0) {
            int availWords = Math.max(0, srcWords.length - startWord);
            int copyWords = Math.min(wordCount, availWords);
            if (copyWords > 0) {
                MemorySegment.copy(srcWords, startWord, out, ValueLayout.JAVA_LONG, 0L, copyWords);
            }
            for (int w = copyWords; w < wordCount; w++) {
                out.setAtIndex(ValueLayout.JAVA_LONG, w, 0L);
            }
        } else {
            for (int i = 0; i < wordCount; i++) {
                long lo = (startWord + i < srcWords.length) ? srcWords[startWord + i] >>> bitOffset : 0L;
                long hi = (startWord + i + 1 < srcWords.length) ? srcWords[startWord + i + 1] << (64 - bitOffset) : 0L;
                out.setAtIndex(ValueLayout.JAVA_LONG, i, lo | hi);
            }
        }
        int trailing = span & 63;
        if (trailing != 0) {
            long lastWord = out.getAtIndex(ValueLayout.JAVA_LONG, wordCount - 1);
            out.setAtIndex(ValueLayout.JAVA_LONG, wordCount - 1, lastWord & ((1L << trailing) - 1));
        }
    }

    @Override
    public void close() {
        weightsByProviderKey.clear();
        scorersByCollectorKey.clear();
    }

    private SegmentReader unwrapSegmentReader(LeafReader reader) {
        LeafReader current = reader;
        while (current instanceof FilterLeafReader flr) {
            current = flr.getDelegate();
        }
        return (SegmentReader) current;
    }

    private static final class ScorerHandle {
        final Scorer scorer;
        final int partitionMinDoc;
        final int partitionMaxDoc;
        /** Segment live docs at collector creation ({@code null} = no deletions in the segment). */
        final org.apache.lucene.util.Bits liveDocs;
        /**
         * True for collectors of the reserved match-all provider (deleted-doc filtering path):
         * {@code collectDocs} emits the live-docs bitset directly, {@code scorer} is {@code null}.
         */
        final boolean emitLiveDocs;
        int currentDoc = -1;

        ScorerHandle(Scorer scorer, int partitionMinDoc, int partitionMaxDoc, org.apache.lucene.util.Bits liveDocs, boolean emitLiveDocs) {
            this.scorer = scorer;
            this.partitionMinDoc = partitionMinDoc;
            this.partitionMaxDoc = partitionMaxDoc;
            this.liveDocs = liveDocs;
            this.emitLiveDocs = emitLiveDocs;
        }
    }
}
