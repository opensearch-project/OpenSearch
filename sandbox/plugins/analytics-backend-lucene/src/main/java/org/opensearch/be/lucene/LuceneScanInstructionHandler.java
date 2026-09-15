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
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.analytics.spi.ArrowBatchSourcePlan;
import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.analytics.spi.CommonExecutionContext;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.ShardScanInstructionNode;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.List;

/**
 * Lucene-side shard-scan instruction handler. Reads a {@link ShardScanInstructionNode}
 * produced for a Lucene {@code StagePlan}, acquires the shard's Lucene reader, decodes its
 * {@link LuceneFragmentWirePlan}, compiles the optional filter to a Lucene {@link Query},
 * and returns a {@link LuceneSearcherState} for
 * {@link LuceneSearchExecEngine} to execute.
 *
 * <p>Empty {@code fragmentBytes} → {@link MatchAllDocsQuery} (count(*) over the whole shard).
 *
 * @opensearch.internal
 */
final class LuceneScanInstructionHandler implements FragmentInstructionHandler<ShardScanInstructionNode> {

    private static final Logger LOGGER = LogManager.getLogger(LuceneScanInstructionHandler.class);

    private final LucenePlugin plugin;

    LuceneScanInstructionHandler(LucenePlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public BackendExecutionContext apply(
        ShardScanInstructionNode node,
        CommonExecutionContext commonContext,
        BackendExecutionContext backendContext
    ) {
        ShardScanExecutionContext shardCtx = (ShardScanExecutionContext) commonContext;
        IndexReaderProvider.Reader reader = shardCtx.getReader();
        LuceneReader luceneReader = reader.getReader(plugin.getDataFormat(), LuceneReader.class);
        if (luceneReader == null) {
            throw new IllegalStateException("Lucene-driver fragment dispatched to a shard with no LuceneReader");
        }
        // Shared per-reader searcher (see LuceneReader#searcher).
        IndexSearcher searcher = luceneReader.searcher(shardCtx.getQueryCache(), shardCtx.getQueryCachingPolicy());
        Decoded decoded = decodeFragmentBytes(shardCtx, searcher);
        LOGGER.debug(
            "[lucene-scan] shardId={} filterQuery={} columnNames={} arrowSourcePlan={}",
            shardCtx.getShardId(),
            decoded.filterQuery,
            decoded.outputNames,
            decoded.arrowSourcePlan != null
        );
        return new LuceneSearcherState(searcher, decoded.filterQuery, decoded.outputNames, decoded.arrowSourcePlan);
    }

    private Decoded decodeFragmentBytes(ShardScanExecutionContext shardCtx, IndexSearcher searcher) {
        byte[] bytes = shardCtx.getFragmentBytes();
        if (bytes == null || bytes.length == 0) {
            return new Decoded(new MatchAllDocsQuery(), List.of(), null);
        }
        LuceneFragmentWirePlan wirePlan = LuceneFragmentWirePlan.fromBytes(bytes);
        QueryBuilder queryBuilder = wirePlan.filterQuery(shardCtx.getNamedWriteableRegistry());
        Query filterQuery;
        if (queryBuilder == null) {
            filterQuery = new MatchAllDocsQuery();
        } else {
            QueryShardContext queryContext = LuceneAnalyticsBackendPlugin.buildMinimalQueryShardContext(shardCtx, searcher);
            try {
                filterQuery = LuceneQueryConversionUtils.rewriteFieldExistsForSecondary(
                    queryBuilder.toQuery(queryContext),
                    field -> searcher.getIndexReader().leaves().stream().anyMatch(leaf -> {
                        var fieldInfo = leaf.reader().getFieldInfos().fieldInfo(field);
                        return fieldInfo != null && fieldInfo.getDocValuesType() != DocValuesType.NONE;
                    })
                );
            } catch (IOException e) {
                throw new IllegalStateException("Failed to compile Lucene fragment filter", e);
            }
        }
        return new Decoded(filterQuery, wirePlan.outputNames(), wirePlan.arrowSourcePlan());
    }

    private record Decoded(Query filterQuery, List<String> outputNames, ArrowBatchSourcePlan arrowSourcePlan) {
    }
}
