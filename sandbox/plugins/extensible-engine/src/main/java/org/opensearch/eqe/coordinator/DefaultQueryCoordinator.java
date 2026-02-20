/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.eqe.coordinator;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.eqe.action.ShardQueryAction;
import org.opensearch.eqe.action.ShardQueryRequest;
import org.opensearch.eqe.action.ShardQueryResponse;
import org.opensearch.eqe.calcite.SubstraitConverter;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportResponseHandler;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Default implementation of {@link QueryCoordinator}.
 *
 * // TODO: Add proper distribution / stage building, for now simply fans out to a single index
 * <p>Fans out plan fragments to all data nodes via streaming transport,
 * collects shard responses, and merges them into a single Arrow
 * {@link VectorSchemaRoot}.
 */
public class DefaultQueryCoordinator implements QueryCoordinator {

    private static final Logger logger = LogManager.getLogger(DefaultQueryCoordinator.class);

    private final StreamTransportService streamTransportService;
    private final ClusterService clusterService;
    private final BufferAllocator allocator;
    private final SubstraitConverter substraitConverter;

    public DefaultQueryCoordinator(
            StreamTransportService streamTransportService,
            ClusterService clusterService,
            BufferAllocator allocator) {
        this.streamTransportService = streamTransportService;
        this.clusterService = clusterService;
        this.allocator = allocator;
        this.substraitConverter = new SubstraitConverter();
    }

    @Override
    public CompletableFuture<VectorSchemaRoot> execute(RelNode logicalPlan, Task parentTask) {
        Set<String> indices = extractIndexNames(logicalPlan);
        byte[] planBytes = substraitConverter.convert(logicalPlan);
        List<ShardTarget> targets = resolveShardTargets(indices);
        return fanOut(planBytes, targets, parentTask).thenApply(this::mergeResponses);
    }

    /**
     * Extract index names from all TableScan nodes in the RelNode tree.
     */
    private Set<String> extractIndexNames(RelNode root) {
        Set<String> indices = new LinkedHashSet<>();
        new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (node instanceof TableScan) {
                    List<String> qualifiedName = node.getTable().getQualifiedName();
                    indices.add(qualifiedName.get(qualifiedName.size() - 1));
                }
                super.visit(node, ordinal, parent);
            }
        }.go(root);
        return indices;
    }

    /**
     * Resolve index names to shard targets using {@link org.opensearch.cluster.routing.OperationRouting}.
     * Uses the same shard resolution as TransportSearchAction — respects preference,
     * adaptive replica selection, and awareness attributes.
     */
    private List<ShardTarget> resolveShardTargets(Set<String> indexNames) {
        ClusterState state = clusterService.state();
        String[] concreteIndices = indexNames.toArray(new String[0]);

        GroupShardsIterator<ShardIterator> shardIterators = clusterService.operationRouting()
            .searchShards(state, concreteIndices, null, null);

        List<ShardTarget> targets = new ArrayList<>();
        for (ShardIterator shardIt : shardIterators) {
            ShardRouting routing = shardIt.nextOrNull();
            if (routing == null) {
                logger.warn("No active shard copy for {}, skipping", shardIt.shardId());
                continue;
            }
            DiscoveryNode node = state.nodes().get(routing.currentNodeId());
            if (node == null) {
                logger.warn("Node {} for shard {} not found, skipping", routing.currentNodeId(), routing.shardId());
                continue;
            }
            targets.add(new ShardTarget(routing.shardId(), node));
        }

        return targets;
    }

    /**
     * Fan out plan bytes to specific shard targets via streaming transport.
     * Uses {@link StreamTransportService#sendChildRequest} to link shard requests
     * to the parent task for cancellation propagation and task tree visibility.
     */
    private CompletableFuture<List<ShardQueryResponse>> fanOut(byte[] planBytes, List<ShardTarget> targets, Task parentTask) {
        final CompletableFuture<List<ShardQueryResponse>> future = new CompletableFuture<>();

        if (targets.isEmpty()) {
            future.complete(List.of());
            return future;
        }

        List<ShardQueryResponse> allResponses = new CopyOnWriteArrayList<>();
        AtomicInteger remaining = new AtomicInteger(targets.size());

        for (ShardTarget target : targets) {
            ShardQueryRequest request = new ShardQueryRequest(target.shardId, planBytes);
            Transport.Connection connection = streamTransportService.getConnection(target.node);

            StreamTransportResponseHandler<ShardQueryResponse> handler = new StreamTransportResponseHandler<ShardQueryResponse>() {
                @Override
                public void handleStreamResponse(StreamTransportResponse<ShardQueryResponse> streamResponse) {
                    try {
                        ShardQueryResponse response;
                        while ((response = streamResponse.nextResponse()) != null) {
                            allResponses.add(response);
                        }
                        streamResponse.close();
                    } catch (Exception e) {
                        streamResponse.cancel("Error processing stream", e);
                        future.completeExceptionally(e);
                        return;
                    }
                    if (remaining.decrementAndGet() == 0) {
                        future.complete(new ArrayList<>(allResponses));
                    }
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.error("Transport exception from shard {} on node {}", target.shardId, target.node.getId(), exp);
                    future.completeExceptionally(exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public ShardQueryResponse read(StreamInput in) throws IOException {
                    return new ShardQueryResponse(in);
                }
            };

            streamTransportService.sendChildRequest(
                connection,
                ShardQueryAction.NAME,
                request,
                parentTask,
                handler
            );
        }

        return future;
    }

    /**
     * Merge shard responses into a single VectorSchemaRoot.
     */
    private VectorSchemaRoot mergeResponses(List<ShardQueryResponse> responses) {
        if (responses.isEmpty()) {
            return VectorSchemaRoot.create(new Schema(List.of()), allocator);
        }

        List<String> columnNames = responses.get(0).getColumns();

        List<Object[]> allRows = new ArrayList<>();
        for (ShardQueryResponse resp : responses) {
            if (resp.hasError()) {
                throw new RuntimeException("Shard error: " + resp.getError());
            }
            allRows.addAll(resp.getRows());
        }

        List<Field> fields = new ArrayList<>();
        for (int col = 0; col < columnNames.size(); col++) {
            ArrowType arrowType = inferArrowType(allRows, col);
            fields.add(new Field(columnNames.get(col), FieldType.nullable(arrowType), null));
        }

        Schema schema = new Schema(fields);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();

        for (int rowIdx = 0; rowIdx < allRows.size(); rowIdx++) {
            Object[] row = allRows.get(rowIdx);
            for (int colIdx = 0; colIdx < columnNames.size(); colIdx++) {
                setVectorValue(root.getVector(colIdx), rowIdx, row[colIdx]);
            }
        }

        root.setRowCount(allRows.size());
        return root;
    }

    private ArrowType inferArrowType(List<Object[]> rows, int colIdx) {
        for (Object[] row : rows) {
            if (colIdx < row.length && row[colIdx] != null) {
                Object val = row[colIdx];
                if (val instanceof Long || val instanceof Integer) {
                    return new ArrowType.Int(64, true);
                } else if (val instanceof Double || val instanceof Float) {
                    return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
                } else if (val instanceof Boolean) {
                    return new ArrowType.Bool();
                } else {
                    return new ArrowType.Utf8();
                }
            }
        }
        return new ArrowType.Utf8();
    }

    private void setVectorValue(FieldVector vector, int index, Object value) {
        if (value == null) {
            return;
        }
        if (vector instanceof VarCharVector) {
            ((VarCharVector) vector).setSafe(index, value.toString().getBytes(StandardCharsets.UTF_8));
        } else if (vector instanceof BigIntVector) {
            ((BigIntVector) vector).setSafe(index, ((Number) value).longValue());
        } else if (vector instanceof Float8Vector) {
            ((Float8Vector) vector).setSafe(index, ((Number) value).doubleValue());
        } else if (vector instanceof BitVector) {
            ((BitVector) vector).setSafe(index, (Boolean) value ? 1 : 0);
        }
    }

    /**
     * A shard target: the specific shard and the node that holds it.
     */
    private static final class ShardTarget {
        final ShardId shardId;
        final DiscoveryNode node;

        ShardTarget(ShardId shardId, DiscoveryNode node) {
            this.shardId = shardId;
            this.node = node;
        }
    }
}
