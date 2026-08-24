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
import org.opensearch.analytics.spi.ArrowBatchSourceFactory.ColumnKind;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory.InputColumn;
import org.opensearch.analytics.spi.ArrowBatchSourcePlan;
import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.analytics.spi.CommonExecutionContext;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.ShardScanInstructionNode;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;

/**
 * Lucene-side shard-scan instruction handler. Reads a {@link ShardScanInstructionNode}
 * produced for a Lucene {@code StagePlan}, acquires the shard's Lucene reader, deserialises
 * the filter {@link QueryBuilder} from {@code ShardScanExecutionContext.getFragmentBytes()},
 * compiles it to a Lucene {@link Query}, and returns a {@link LuceneSearcherState} for
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
            decoded.columnNames,
            decoded.arrowSourcePlan != null
        );
        return new LuceneSearcherState(searcher, decoded.filterQuery, decoded.columnNames, decoded.arrowSourcePlan);
    }

    /**
     * Deserializes the wire format produced by {@link LuceneFragmentConvertor#convertFragment}:
     * {@code [columnNames String[]] [hasFilter boolean] [QueryBuilder NamedWriteable]?}.
     * Empty bytes → no filter, no column names (legacy/defensive fallback that shouldn't
     * happen on the Lucene-driver path but stays safe if the wire shape ever drifts).
     */
    private Decoded decodeFragmentBytes(ShardScanExecutionContext shardCtx, IndexSearcher searcher) {
        byte[] bytes = shardCtx.getFragmentBytes();
        if (bytes == null || bytes.length == 0) {
            return new Decoded(new MatchAllDocsQuery(), java.util.List.of(), null);
        }
        try (StreamInput rawInput = StreamInput.wrap(bytes)) {
            StreamInput input = new NamedWriteableAwareStreamInput(rawInput, shardCtx.getNamedWriteableRegistry());
            java.util.List<String> columnNames = input.readStringList();
            boolean arrowSource = columnNames.isEmpty() == false
                && LuceneFragmentConvertor.ARROW_SOURCE_PLAN_MARKER.equals(columnNames.getFirst());
            boolean hasFilter = input.readBoolean();
            Query filterQuery;
            if (hasFilter) {
                QueryShardContext qsc = LuceneAnalyticsBackendPlugin.buildMinimalQueryShardContext(shardCtx, searcher);
                QueryBuilder queryBuilder = input.readNamedWriteable(QueryBuilder.class);
                Query compiledQuery = queryBuilder.toQuery(qsc);
                filterQuery = LuceneQueryConversionUtils.rewriteFieldExistsForSecondary(
                    compiledQuery,
                    field -> searcher.getIndexReader().leaves().stream().anyMatch(leaf -> {
                        var fieldInfo = leaf.reader().getFieldInfos().fieldInfo(field);
                        return fieldInfo != null && fieldInfo.getDocValuesType() != DocValuesType.NONE;
                    })
                );
            } else {
                filterQuery = new MatchAllDocsQuery();
            }
            if (arrowSource) {
                int position = 1;
                byte[] planBytes = java.util.Base64.getDecoder().decode(columnNames.get(position++));
                String inputId = columnNames.get(position++);
                int inputCount = Integer.parseInt(columnNames.get(position++));
                java.util.List<InputColumn> inputColumns = new java.util.ArrayList<>(inputCount);
                for (int i = 0; i < inputCount; i++) {
                    String name = columnNames.get(position++);
                    ColumnKind kind = ColumnKind.valueOf(columnNames.get(position++));
                    inputColumns.add(new InputColumn(name, kind));
                }
                int outputCount = Integer.parseInt(columnNames.get(position++));
                java.util.List<String> outputNames = new java.util.ArrayList<>(outputCount);
                for (int i = 0; i < outputCount; i++) {
                    outputNames.add(columnNames.get(position++));
                }
                if (position != columnNames.size()) {
                    throw new IllegalStateException("Unexpected trailing Arrow source plan metadata");
                }
                return new Decoded(filterQuery, outputNames, new ArrowBatchSourcePlan(inputId, planBytes, inputColumns));
            }
            return new Decoded(filterQuery, columnNames, null);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to deserialize Lucene-driver fragment bytes", e);
        }
    }

    private record Decoded(Query filterQuery, java.util.List<String> columnNames, ArrowBatchSourcePlan arrowSourcePlan) {
    }
}
