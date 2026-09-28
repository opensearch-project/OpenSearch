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
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LiveDocs;
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
    /** Provider keys created from {@link #LIVE_DOCS_MATCH_ALL_ANNOTATION_ID} — collectDocs takes the live-docs fast path. */
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
        // Deleted-doc filtering: always register the reserved match-all query (even when
        // `expressions` is empty) so its collector can emit the segment's live docs. A single map
        // entry; no Weight is created unless the id is actually used.
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
            Bits liveDocs = leaf.reader().getLiveDocs();
            // Match-all provider: collectDocs emits the segment's live docs directly via fast path (fillLiveDocsWords).
            boolean matchAllProvider = liveDocsProviderKeys.contains(providerKey);
            Scorer scorer = matchAllProvider ? null : weight.scorer(leaf);
            // Match-all uses the fast path, so no liveIntersection iterator is required.
            DocIdSetIterator liveIntersection = matchAllProvider ? null : liveIntersection(scorer, liveDocs);
            int collectorKey = nextCollectorKey.getAndIncrement();
            // Keep only what collectDocs reads: a pre-filtered (dense-delete) leaf walks liveIntersection
            // alone, so drop the scorer and liveDocs; the raw-scorer path keeps both, and match-all keeps
            // liveDocs for the fast path.
            scorersByCollectorKey.put(
                collectorKey,
                new ScorerHandle(
                    liveIntersection == null ? scorer : null,
                    minDoc,
                    maxDoc,
                    liveIntersection == null ? liveDocs : null,
                    matchAllProvider,
                    liveIntersection
                )
            );
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

    /**
     * Intersect the scorer with live docs for dense-delete leaves ({@code DenseLiveDocs}, which expose a
     * backing {@link FixedBitSet} of live docs): lead a conjunction with it so {@link ConjunctionUtils}
     * skips the deleted majority. Returns {@code null} when there's nothing to pre-filter — no scorer,
     * no deletions, or sparse deletions with no FixedBitSet backing — in which case collectDocs uses the
     * raw scorer and drops deleted docs per-doc via {@code liveDocs.get}.
     */
    private static DocIdSetIterator liveIntersection(Scorer scorer, Bits liveDocs) {
        if (scorer != null && liveDocs instanceof LiveDocs ld) {
            FixedBitSet liveBits = BitSetIterator.getFixedBitSetOrNull(ld.liveDocsIterator());
            if (liveBits != null) {
                return ConjunctionUtils.intersectIterators(
                    List.of(new BitSetIterator(liveBits, liveBits.cardinality()), scorer.iterator())
                );
            }
        }
        return null;
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
        int wordCount = (span + 63) >>> 6;
        if (handle.matchAllProvider) {
            // Match-all provider: emit the live docs directly (see fillLiveDocsWords). Match-all never
            // exhausts, so nextDoc = maxDoc.
            fillLiveDocsWords(handle.liveDocs, minDoc, span, wordCount, out);
            return ((long) maxDoc << 32) | (wordCount & 0xFFFFFFFFL);
        }
        FixedBitSet bits = new FixedBitSet(span);
        int nextDoc = Integer.MAX_VALUE;
        if (handle.scorer != null || handle.liveIntersection != null) {
            int scanFrom = Math.max(minDoc, handle.partitionMinDoc);
            int scanTo = Math.min(maxDoc, handle.partitionMaxDoc);

            if (scanFrom < scanTo) {
                try {
                    // Dense-delete leaves walk the scorer ∩ live-docs conjunction (already excludes deleted);
                    // otherwise walk the raw scorer and drop deleted docs per-doc via liveDocs.get below.
                    boolean preFiltered = handle.liveIntersection != null;
                    DocIdSetIterator iterator = preFiltered ? handle.liveIntersection : handle.scorer.iterator();
                    int docId = handle.currentDoc;
                    if (docId != DocIdSetIterator.NO_MORE_DOCS) {
                        if (docId < scanFrom) {
                            docId = iterator.advance(scanFrom);
                        }
                        while (docId != DocIdSetIterator.NO_MORE_DOCS && docId < scanTo) {
                            if (preFiltered || handle.liveDocs == null || handle.liveDocs.get(docId)) {
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
     * Write {@code span} all-alive bits (all-ones, trailing partial word masked to {@code span & 63})
     * into {@code out} as {@code wordCount} LSB-first longs. Used when a segment has no deletions —
     * every doc in range is live — so no liveDocs lookup is needed.
     */
    private static void fillAllAliveWords(MemorySegment out, int span, int wordCount) {
        for (int w = 0; w < wordCount; w++) {
            out.setAtIndex(ValueLayout.JAVA_LONG, w, -1L);
        }
        int trailing = span & 63;
        if (trailing != 0) {
            out.setAtIndex(ValueLayout.JAVA_LONG, wordCount - 1, (1L << trailing) - 1);
        }
    }

    /**
     * Pack the LIVE-docs slice {@code [minDoc, minDoc+span)} into {@code out} as {@code wordCount} LSB-first
     * longs (set bit == live), without scorer iteration. No deletions ({@code liveDocs == null}) emits
     * all-ones; dense segments word-copy the backing {@link FixedBitSet} (O(words)); sparse segments fill
     * all-alive then clear the O(deletions) deleted bits. Caller guarantees {@code span > 0} and that a
     * non-null {@code liveDocs} is a {@link LiveDocs} (the codec contract for a segment with deletions).
     */
    private static void fillLiveDocsWords(Bits liveDocs, int minDoc, int span, int wordCount, MemorySegment out) {
        if (liveDocs == null) {
            // Segment has no deletions — every doc is live (all-ones, trailing word masked).
            fillAllAliveWords(out, span, wordCount);
            return;
        }
        LiveDocs ld = (LiveDocs) liveDocs;
        FixedBitSet liveBits = org.apache.lucene.util.BitSetIterator.getFixedBitSetOrNull(ld.liveDocsIterator());
        if (liveBits != null) {
            // Dense deletions: word-copy the backing live FixedBitSet.
            copyLiveWords(liveBits, out, minDoc, span, wordCount);
            return;
        }
        // Sparse deletions: fill all-alive, then clear the O(deletions) deleted bits.
        fillAllAliveWords(out, span, wordCount);
        int maxDoc = minDoc + span;
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
        } catch (IOException e) {
            LOGGER.warn("IOException during fillLiveDocsWords, returning partial live-docs bitset", e);
        }
    }

    /**
     * Copy the LIVE-docs slice {@code [effectiveMinDoc, effectiveMinDoc + span)} of a
     * {@link FixedBitSet} into {@code out} as {@code wordCount} packed longs (set bit == live).
     *
     * <p>Over-copy is safe without per-word clearing: bits past {@code fbs.length()} are 0
     * (FixedBitSet invariant), and only the final word can hold bits beyond {@code span} — the
     * trailing mask below clears it.
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
        liveDocsProviderKeys.clear();
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
        /** The predicate scorer for the raw-scorer path; {@code null} for match-all and dense-delete (pre-filtered) collectors, which don't walk it. */
        final Scorer scorer;
        final int partitionMinDoc;
        final int partitionMaxDoc;
        /** Live docs for the fast path / per-doc {@code get}; {@code null} when unused (dense-delete predicate) or when the segment has no deletions. */
        final Bits liveDocs;
        /** True for the reserved match-all provider: collectDocs emits live docs directly via fillLiveDocsWords. */
        final boolean matchAllProvider;
        /** Persistent scorer ∩ live-docs conjunction for dense-delete leaves; {@code null} → walk the raw scorer + liveDocs.get. */
        final DocIdSetIterator liveIntersection;
        int currentDoc = -1;

        ScorerHandle(
            Scorer scorer,
            int partitionMinDoc,
            int partitionMaxDoc,
            Bits liveDocs,
            boolean matchAllProvider,
            DocIdSetIterator liveIntersection
        ) {
            this.scorer = scorer;
            this.partitionMinDoc = partitionMinDoc;
            this.partitionMaxDoc = partitionMaxDoc;
            this.liveDocs = liveDocs;
            this.matchAllProvider = matchAllProvider;
            this.liveIntersection = liveIntersection;
        }
    }
}
