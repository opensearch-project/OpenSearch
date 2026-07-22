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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.analytics.spi.DelegatedExpression;
import org.opensearch.analytics.spi.FilterDelegationHandle;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
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

    /**
     * Probe-scorer cost gate threshold (percentage, 0-100). When a scorer's
     * estimated cost exceeds this percentage of the segment's maxDoc, the probe
     * scan is skipped (all RGs marked as matching). Default 5%.
     * <p>
     * Dynamically updatable via cluster setting:
     * {@code PUT _cluster/settings {"transient":{"analytics.lucene.probe_threshold_percent": 10}}}
     */
    public static final Setting<Integer> PROBE_THRESHOLD_PERCENT_SETTING = Setting.intSetting(
        "analytics.lucene.probe_threshold_percent",
        1,
        0,
        100,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static volatile int probeThresholdPercent = 1;

    static void setProbeThresholdPercent(int percent) {
        probeThresholdPercent = percent;
        LOGGER.info("[probe-skip] Probe threshold updated to {}%", percent);
    }

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
        this.searcher = queryShardContext.searcher();
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
            LOGGER.debug("[scf] createProvider annotationId={} → providerKey={}", annotationId, providerKey);
            return providerKey;
        } catch (IOException exception) {
            LOGGER.error("createProvider failed for annotationId=" + annotationId, exception);
            return -1;
        }
    }

    @Override
    public int createCollector(int providerKey, long writerGeneration, int minDoc, int maxDoc) {
        // Legacy entry point — preserves old behavior where null scorer
        // returns a valid collectorKey (collectDocs returns empty bitsets).
        long packed = createCollectorWithCost(providerKey, writerGeneration, minDoc, maxDoc);
        if (packed == -1L) return -1;
        if (packed == -2L) {
            // Segment empty — store null scorer with a valid key (old behavior)
            int collectorKey = nextCollectorKey.getAndIncrement();
            scorersByCollectorKey.put(collectorKey, new ScorerHandle(null, minDoc, maxDoc));
            return collectorKey;
        }
        return (int) (packed & 0xFFFFFFFFL);
    }

    @Override
    public long createCollectorWithCost(int providerKey, long writerGeneration, int minDoc, int maxDoc) {
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
            Scorer scorer = weight.scorer(leaf);
            if (scorer == null) {
                return -2L;
            }
            long cost = scorer.iterator().cost();
            int collectorKey = nextCollectorKey.getAndIncrement();
            scorersByCollectorKey.put(collectorKey, new ScorerHandle(scorer, minDoc, maxDoc));
            // Pack: upper 32 bits = cost (capped), lower 32 bits = collectorKey
            int costCapped = (int) Math.min(cost, Integer.MAX_VALUE);
            LOGGER.debug(
                "[probe-skip] COST: cost={} segMaxDoc={} ({}%) collectorKey={}",
                cost,
                leaf.reader().maxDoc(),
                (cost * 100) / leaf.reader().maxDoc(),
                collectorKey
            );
            return ((long) costCapped << 32) | (collectorKey & 0xFFFFFFFFL);
        } catch (IOException exception) {
            LOGGER.error(
                "createCollector failed for providerKey=" + providerKey + ", writerGeneration=" + writerGeneration + ", segment=" + segName,
                exception
            );
            return -1;
        }
    }

    @Override
    public long createCollectorWithProbe(
        int providerKey,
        long writerGeneration,
        int minDoc,
        int maxDoc,
        int[] rgMins,
        int[] rgMaxs,
        byte[] outMatch
    ) {
        Weight weight = weightsByProviderKey.get(providerKey);
        if (weight == null) {
            return -1L;
        }
        String segName = generationToSegmentName.get(writerGeneration);
        if (segName == null) {
            LOGGER.error(
                "createCollectorWithProbe: no Lucene segment for writer_generation={} (providerKey={}). Known generations: {}",
                writerGeneration,
                providerKey,
                generationToSegmentName.keySet()
            );
            return -1L;
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
                "createCollectorWithProbe: segment name [{}] not found in leaves (writerGeneration={}, providerKey={})",
                segName,
                writerGeneration,
                providerKey
            );
            return -1L;
        }

        try {
            // Create the collection scorer
            Scorer scorer = weight.scorer(leaf);
            if (scorer == null) {
                // No docs match in this segment — signal to Rust
                Arrays.fill(outMatch, (byte) 0);
                return -2L;
            }

            // Read cost before storing scorer — cost() is a pure metadata getter
            // on the iterator but we read it early to avoid any concern about
            // state coupling with later collectDocs calls.
            long cost = scorer.iterator().cost();

            int collectorKey = nextCollectorKey.getAndIncrement();
            scorersByCollectorKey.put(collectorKey, new ScorerHandle(scorer, minDoc, maxDoc));

            // Cost-based gate: skip the probe when cost exceeds threshold% of segment.
            // At that density, docs are spread across nearly all RGs — probing would
            // return all-1s. Threshold configurable via cluster setting (default 1%).
            long segMaxDoc = leaf.reader().maxDoc();
            int threshold = probeThresholdPercent;
            if (threshold > 0 && cost * 100 > segMaxDoc * threshold) {
                Arrays.fill(outMatch, (byte) 1);
                long packed = ((long) 0 << 32) | (collectorKey & 0xFFFFFFFFL);
                return packed;
            }

            // Selective query — create a probe scorer for RG-level skip detection
            int firstDoc = -1;
            Scorer probeScorer = weight.scorer(leaf);
            if (probeScorer != null) {
                DocIdSetIterator probeIter = probeScorer.iterator();
                int probeDoc = probeIter.nextDoc();
                firstDoc = probeDoc;

                for (int i = 0; i < rgMins.length; i++) {
                    if (probeDoc == DocIdSetIterator.NO_MORE_DOCS) {
                        outMatch[i] = 0;
                        continue;
                    }
                    if (probeDoc < rgMins[i]) {
                        probeDoc = probeIter.advance(rgMins[i]);
                    }
                    if (probeDoc < rgMaxs[i]) {
                        outMatch[i] = (byte) 1;
                    } else {
                        outMatch[i] = (byte) 0;
                    }
                }
            } else {
                Arrays.fill(outMatch, (byte) 1);
            }

            long packed = ((long) Math.max(firstDoc, 0) << 32) | (collectorKey & 0xFFFFFFFFL);
            return packed;
        } catch (IOException exception) {
            LOGGER.error(
                "createCollectorWithProbe failed for providerKey=" + providerKey + ", writerGeneration=" + writerGeneration,
                exception
            );
            return -1L;
        }
    }

    @Override
    public long probeCollector(int providerKey, long writerGeneration, int[] rgMins, int[] rgMaxs, byte[] outMatch) {
        Weight weight = weightsByProviderKey.get(providerKey);
        if (weight == null) {
            return -1L;
        }
        String segName = generationToSegmentName.get(writerGeneration);
        if (segName == null) {
            return -1L;
        }
        LeafReaderContext leaf = null;
        for (LeafReaderContext lrc : leaves) {
            if (unwrapSegmentReader(lrc.reader()).getSegmentInfo().info.name.equals(segName)) {
                leaf = lrc;
                break;
            }
        }
        if (leaf == null) {
            return -1L;
        }

        try {
            Scorer probeScorer = weight.scorer(leaf);
            if (probeScorer == null) {
                Arrays.fill(outMatch, (byte) 0);
                return 0L;
            }
            DocIdSetIterator probeIter = probeScorer.iterator();
            int probeDoc = probeIter.nextDoc();
            int skipped = 0;

            for (int i = 0; i < rgMins.length; i++) {
                if (probeDoc == DocIdSetIterator.NO_MORE_DOCS) {
                    outMatch[i] = 0;
                    skipped++;
                    continue;
                }
                if (probeDoc < rgMins[i]) {
                    probeDoc = probeIter.advance(rgMins[i]);
                }
                if (probeDoc < rgMaxs[i]) {
                    outMatch[i] = (byte) 1;
                } else {
                    outMatch[i] = (byte) 0;
                    skipped++;
                }
            }
            return skipped;
        } catch (IOException exception) {
            LOGGER.error("probeCollector failed for providerKey=" + providerKey, exception);
            return -1L;
        }
    }

    @Override
    public boolean isCancelled() {
        return isCancelledSupplier != null && isCancelledSupplier.getAsBoolean();
    }

    @Override
    public int collectDocs(int collectorKey, int minDoc, int maxDoc, MemorySegment out) {
        ScorerHandle handle = scorersByCollectorKey.get(collectorKey);
        if (handle == null) {
            return -1;
        }
        if (maxDoc <= minDoc) {
            return 0;
        }
        int span = maxDoc - minDoc;
        FixedBitSet bits = new FixedBitSet(span);

        if (handle.scorer != null) {
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
                            bits.set(docId - minDoc);
                            docId = iterator.nextDoc();
                        }
                        handle.currentDoc = docId;
                    }
                } catch (IOException exception) {
                    LOGGER.warn("IOException during collectDocs, returning partial bitset", exception);
                }
            }
        }

        long[] words = bits.getBits();
        int wordCount = (span + 63) >>> 6;
        MemorySegment.copy(words, 0, out, ValueLayout.JAVA_LONG, 0, wordCount);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "[scf] collectDocs collectorKey={} range=[{},{}) → cardinality={} words={}",
                collectorKey,
                minDoc,
                maxDoc,
                bits.cardinality(),
                wordCount
            );
        }
        return wordCount;
    }

    @Override
    public void releaseCollector(int collectorKey) {
        scorersByCollectorKey.remove(collectorKey);
    }

    @Override
    public void releaseProvider(int providerKey) {
        weightsByProviderKey.remove(providerKey);
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
        int currentDoc = -1;

        ScorerHandle(Scorer scorer, int partitionMinDoc, int partitionMaxDoc) {
            this.scorer = scorer;
            this.partitionMinDoc = partitionMinDoc;
            this.partitionMaxDoc = partitionMaxDoc;
        }
    }
}
