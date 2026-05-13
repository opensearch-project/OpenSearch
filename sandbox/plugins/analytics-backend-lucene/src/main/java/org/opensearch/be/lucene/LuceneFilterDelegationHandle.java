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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
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
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
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

import static org.opensearch.be.lucene.index.LuceneWriter.WRITER_GENERATION_ATTRIBUTE;

/**
 * Lucene implementation of {@link FilterDelegationHandle}. Compiles delegated expressions
 * into Lucene Queries, creates Weights on demand, and produces bitsets via Scorers.
 *
 * <p>Segments are resolved by <b>writer generation</b> (the stable per-segment
 * identifier), not by positional ordinal.
 * {@link org.opensearch.be.lucene.index.LuceneWriter#WRITER_GENERATION_ATTRIBUTE}
 * stamped onto every {@link SegmentCommitInfo} at write and merge time.
 *
 * @opensearch.internal
 */
final class LuceneFilterDelegationHandle implements FilterDelegationHandle {

    private static final Logger LOGGER = LogManager.getLogger(LuceneFilterDelegationHandle.class);

    private final Map<Integer, Query> queriesByAnnotationId;
    private final DirectoryReader directoryReader;
    private final List<LeafReaderContext> leaves;
    /** Writer generation → Lucene leaf index. Built once in the constructor. */
    private final Map<Long, Integer> generationToLeaf;

    private final ConcurrentHashMap<Integer, Weight> weightsByProviderKey = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, ScorerHandle> scorersByCollectorKey = new ConcurrentHashMap<>();
    private final AtomicInteger nextProviderKey = new AtomicInteger(1);
    private final AtomicInteger nextCollectorKey = new AtomicInteger(1);

    // TODO: NamedWriteableRegistry should ideally come from LucenePlugin.createComponents
    // instead of being threaded through ShardScanExecutionContext from Core.
    LuceneFilterDelegationHandle(
        List<DelegatedExpression> expressions,
        QueryShardContext queryShardContext,
        DirectoryReader directoryReader,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        assert directoryReader != null : "directoryReader must not be null";
        this.directoryReader = directoryReader;
        this.leaves = directoryReader.leaves();
        this.queriesByAnnotationId = compileQueries(expressions, queryShardContext, namedWriteableRegistry);
        this.generationToLeaf = buildGenerationToLeaf(leaves);
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
                Query query = queryBuilder.toQuery(context);
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

    /**
     * Build {@code writerGeneration → leaf index} from the open {@link DirectoryReader}.
     * Every Lucene leaf must carry a numeric {@link org.opensearch.be.lucene.index.LuceneWriter#WRITER_GENERATION_ATTRIBUTE}
     */
    private static Map<Long, Integer> buildGenerationToLeaf(List<LeafReaderContext> leaves) {
        Map<Long, Integer> out = new HashMap<>(leaves.size());
        for (int leafIdx = 0; leafIdx < leaves.size(); leafIdx++) {
            LeafReaderContext lrc = leaves.get(leafIdx);
            if ((lrc.reader() instanceof SegmentReader) == false) {
                throw new IllegalStateException(
                    "Expected SegmentReader at leaf " + leafIdx + " but got " + lrc.reader().getClass().getName()
                );
            }
            SegmentReader sr = (SegmentReader) lrc.reader();
            SegmentCommitInfo sci = sr.getSegmentInfo();
            String genAttr = sci.info.getAttribute(WRITER_GENERATION_ATTRIBUTE);
            if (genAttr == null) {
                throw new IllegalStateException(
                    "Lucene leaf "
                        + leafIdx
                        + " (segment name="
                        + sci.info.name
                        + ") is missing the "
                        + WRITER_GENERATION_ATTRIBUTE
                        + " attribute. Every segment must be stamped at write/merge time; a missing "
                        + "attribute indicates a LuceneWriter or RowIdRemappingOneMerge regression."
                );
            }
            long gen;
            try {
                gen = Long.parseLong(genAttr);
            } catch (NumberFormatException e) {
                throw new IllegalStateException(
                    "Lucene leaf " + leafIdx + " has a non-numeric " + WRITER_GENERATION_ATTRIBUTE + "=[" + genAttr + "]",
                    e
                );
            }
            Integer prev = out.put(gen, leafIdx);
            if (prev != null) {
                throw new IllegalStateException(
                    "Duplicate writer generation ["
                        + gen
                        + "] seen at Lucene leaves "
                        + prev
                        + " and "
                        + leafIdx
                        + ". Generations must be unique across leaves."
                );
            }
        }
        return out;
    }

    @Override
    public int createProvider(int annotationId) {
        Query query = queriesByAnnotationId.get(annotationId);
        if (query == null) {
            return -1;
        }
        try {
            IndexSearcher searcher = new IndexSearcher(directoryReader);
            Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
            int providerKey = nextProviderKey.getAndIncrement();
            weightsByProviderKey.put(providerKey, weight);
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
        Integer leafIdx = generationToLeaf.get(writerGeneration);
        if (leafIdx == null) {
            LOGGER.error(
                "createCollector: no Lucene leaf for writer_generation={} (providerKey={}). Known generations: {}",
                writerGeneration,
                providerKey,
                generationToLeaf.keySet()
            );
            return -1;
        }
        LeafReaderContext leaf = leaves.get(leafIdx);

        // Partition bounds must sit inside the resolved leaf. If they don't, the caller
        // is addressing a segment whose generation doesn't match its doc count — a
        // regression of the exact bug this class is designed to prevent.
        int leafMaxDoc = leaf.reader().maxDoc();
        assert minDoc >= 0 && minDoc <= maxDoc && maxDoc <= leafMaxDoc : "createCollector(providerKey="
            + providerKey
            + ", writerGeneration="
            + writerGeneration
            + " -> leafOrd="
            + leafIdx
            + "): partition ["
            + minDoc
            + ","
            + maxDoc
            + ") exceeds leaf maxDoc="
            + leafMaxDoc;

        try {
            Scorer scorer = weight.scorer(leaf);
            int collectorKey = nextCollectorKey.getAndIncrement();
            scorersByCollectorKey.put(collectorKey, new ScorerHandle(scorer, minDoc, maxDoc));
            return collectorKey;
        } catch (IOException exception) {
            LOGGER.error(
                "createCollector failed for providerKey=" + providerKey + ", writerGeneration=" + writerGeneration + ", leafOrd=" + leafIdx,
                exception
            );
            return -1;
        }
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
