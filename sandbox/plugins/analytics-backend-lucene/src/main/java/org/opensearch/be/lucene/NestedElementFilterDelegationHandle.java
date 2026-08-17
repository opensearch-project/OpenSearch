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
import org.apache.lucene.index.SortedNumericDocValues;
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
import org.opensearch.index.engine.dataformat.AuxiliaryDataFormat;
import org.opensearch.index.engine.dataformat.DocumentInput;
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
 * {@link FilterDelegationHandle} for the Engine-4 <em>element index</em> ({@code aux__lucene__nested}).
 *
 * <p>Like {@link LuceneFilterDelegationHandle} it compiles a delegated nested-leaf predicate into a
 * Lucene {@link Query} and scores it — but on the element index (one doc per nested element), and the
 * bitset it returns is in <b>parent-row</b> space, not element-doc space. For each matching element doc
 * it reads {@link DocumentInput#NESTED_PARENT_ROW_FIELD} and sets that parent row's bit. The DataFusion
 * scan then consumes those bits exactly as it does the main index's (doc-id == parquet row), so a nested
 * filter becomes a parent {@code RowSelection} with no scan-side change.
 *
 * <p>The row group is identified to {@code createCollector} by its <em>parent</em> writer generation;
 * the element segment for it is at {@link AuxiliaryDataFormat#generationFor(long)} of that generation.
 *
 * @opensearch.internal
 */
final class NestedElementFilterDelegationHandle implements FilterDelegationHandle {

    private static final Logger LOGGER = LogManager.getLogger(NestedElementFilterDelegationHandle.class);

    private final Map<Integer, Query> queriesByAnnotationId;
    private final DirectoryReader directoryReader;
    private final IndexSearcher searcher;
    private final List<LeafReaderContext> leaves;
    private final Map<Long, String> elementGenerationToSegmentName;
    private final BooleanSupplier isCancelledSupplier;

    private final ConcurrentHashMap<Integer, Weight> weightsByProviderKey = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, ElementCollector> collectorsByKey = new ConcurrentHashMap<>();
    private final AtomicInteger nextProviderKey = new AtomicInteger(1);
    private final AtomicInteger nextCollectorKey = new AtomicInteger(1);

    NestedElementFilterDelegationHandle(
        List<DelegatedExpression> expressions,
        QueryShardContext queryShardContext,
        LuceneReader elementReader,
        NamedWriteableRegistry namedWriteableRegistry,
        BooleanSupplier isCancelledSupplier
    ) {
        this.directoryReader = elementReader.directoryReader();
        this.searcher = queryShardContext.searcher();
        this.leaves = directoryReader.leaves();
        this.elementGenerationToSegmentName = elementReader.generationToSegmentName();
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
                Query query = LuceneQueryConversionUtils.rewriteFieldExistsForSecondary(queryBuilder.toQuery(context));
                queries.put(expr.getAnnotationId(), query);
            } catch (IOException exception) {
                throw new IllegalStateException(
                    "Failed to deserialize delegated nested expression for annotationId=" + expr.getAnnotationId(),
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
            return providerKey;
        } catch (IOException exception) {
            LOGGER.error("createProvider failed for nested annotationId=" + annotationId, exception);
            return -1;
        }
    }

    @Override
    public int createCollector(int providerKey, long writerGeneration, int minDoc, int maxDoc) {
        Weight weight = weightsByProviderKey.get(providerKey);
        if (weight == null) {
            return -1;
        }
        // The row group is at the PARENT generation; its element segment is at generationFor(parentGen).
        long elementGeneration = AuxiliaryDataFormat.generationFor(writerGeneration);
        String segName = elementGenerationToSegmentName.get(elementGeneration);
        if (segName == null) {
            LOGGER.error(
                "createCollector: no element segment for parent generation {} (element generation {}). Known: {}",
                writerGeneration,
                elementGeneration,
                elementGenerationToSegmentName.keySet()
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
            LOGGER.error("createCollector: element segment [{}] not found in reader leaves", segName);
            return -1;
        }
        try {
            Scorer scorer = weight.scorer(leaf);
            SortedNumericDocValues parentRow = leaf.reader().getSortedNumericDocValues(DocumentInput.NESTED_PARENT_ROW_FIELD);
            int collectorKey = nextCollectorKey.getAndIncrement();
            collectorsByKey.put(collectorKey, new ElementCollector(scorer, parentRow, minDoc, maxDoc));
            return collectorKey;
        } catch (IOException exception) {
            LOGGER.error("createCollector failed for nested providerKey=" + providerKey, exception);
            return -1;
        }
    }

    @Override
    public boolean isCancelled() {
        return isCancelledSupplier != null && isCancelledSupplier.getAsBoolean();
    }

    @Override
    public int collectDocs(int collectorKey, int minDoc, int maxDoc, MemorySegment out) {
        ElementCollector handle = collectorsByKey.get(collectorKey);
        if (handle == null) {
            return -1;
        }
        if (maxDoc <= minDoc) {
            return 0;
        }
        int span = maxDoc - minDoc;
        FixedBitSet bits = new FixedBitSet(span);

        if (handle.scorer != null && handle.parentRow != null) {
            try {
                // Iterate ALL matching element docs; map each to its parent row via __parent_row__.
                // Element doc ids are element-grain, so we scan the whole element scorer (not [minDoc,maxDoc))
                // and set the PARENT row's bit when it falls in this row group's [minDoc,maxDoc) window.
                DocIdSetIterator iterator = handle.scorer.iterator();
                SortedNumericDocValues parentRow = handle.parentRow;
                for (int elemDoc = iterator.nextDoc(); elemDoc != DocIdSetIterator.NO_MORE_DOCS; elemDoc = iterator.nextDoc()) {
                    if (parentRow.advanceExact(elemDoc) == false) {
                        continue;
                    }
                    long row = parentRow.nextValue();
                    if (row >= minDoc && row < maxDoc) {
                        bits.set((int) (row - minDoc));
                    }
                }
            } catch (IOException exception) {
                LOGGER.warn("IOException during nested collectDocs, returning partial bitset", exception);
            }
        }

        long[] words = bits.getBits();
        int wordCount = (span + 63) >>> 6;
        MemorySegment.copy(words, 0, out, ValueLayout.JAVA_LONG, 0, wordCount);
        return wordCount;
    }

    @Override
    public void releaseCollector(int collectorKey) {
        collectorsByKey.remove(collectorKey);
    }

    @Override
    public void releaseProvider(int providerKey) {
        weightsByProviderKey.remove(providerKey);
    }

    @Override
    public void close() {
        weightsByProviderKey.clear();
        collectorsByKey.clear();
    }

    private SegmentReader unwrapSegmentReader(LeafReader reader) {
        LeafReader current = reader;
        while (current instanceof FilterLeafReader flr) {
            current = flr.getDelegate();
        }
        return (SegmentReader) current;
    }

    /** A scorer over the element segment plus that segment's {@code __parent_row__} doc-values. */
    private static final class ElementCollector {
        final Scorer scorer;
        final SortedNumericDocValues parentRow;
        final int partitionMinDoc;
        final int partitionMaxDoc;

        ElementCollector(Scorer scorer, SortedNumericDocValues parentRow, int partitionMinDoc, int partitionMaxDoc) {
            this.scorer = scorer;
            this.parentRow = parentRow;
            this.partitionMinDoc = partitionMinDoc;
            this.partitionMaxDoc = partitionMaxDoc;
        }
    }
}
