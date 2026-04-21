/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.PendingExecutions;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.StreamingResponseListener;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
import org.opensearch.analytics.exec.action.ShardTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.core.action.ActionResponse;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Per-stage execution for row-producing DATA_NODE stages (scans, filters,
 * partial aggregates). Dispatches shard requests via
 * {@link AnalyticsSearchTransportService#dispatchFragment}, decodes streaming
 * responses through a {@link ResponseCodec}, and feeds the resulting Arrow
 * batches into the stage's output {@link ExchangeSink}.
 *
 * <p>The codec abstracts the wire format: the current {@link RowResponseCodec}
 * converts {@code Object[]} rows to Arrow; a future Arrow IPC codec would
 * import IPC buffers directly with zero conversion. The stage execution logic
 * is format-agnostic.
 *
 * <p>Implements {@link DataProducer} because it writes batches into a sink
 * owned by its parent stage. Does not implement {@link DataConsumer} because
 * it is a leaf stage with no children.
 *
 * <p>Lifecycle: {@code CREATED → RUNNING → SUCCEEDED | FAILED | CANCELLED}.
 * Instances are one-shot: constructed, {@link #start()} called once,
 * listener signaled once, discarded.
 *
 * @opensearch.internal
 */
final class ShardFragmentStageExecution extends AbstractStageExecution implements DataProducer {

    private final AtomicInteger inFlight = new AtomicInteger(0);

    // Immutable config
    private final QueryContext config;
    private final ExchangeSink outputSink;
    private final List<ShardTarget> targets;
    private final Function<ShardTarget, FragmentExecutionRequest> requestBuilder;
    private final AnalyticsSearchTransportService dispatcher;
    private final ResponseCodec<FragmentExecutionResponse> responseCodec;
    private final Map<String, PendingExecutions> pendingPerNode = new ConcurrentHashMap<>();

    ShardFragmentStageExecution(
        Stage stage,
        QueryContext config,
        ExchangeSink outputSink,
        List<ShardTarget> targets,
        Function<ShardTarget, FragmentExecutionRequest> requestBuilder,
        AnalyticsSearchTransportService dispatcher,
        ResponseCodec<FragmentExecutionResponse> responseCodec
    ) {
        super(stage);
        this.config = config;
        this.outputSink = outputSink;
        this.targets = targets;
        this.requestBuilder = requestBuilder;
        this.dispatcher = dispatcher;
        this.responseCodec = responseCodec;
    }

    @Override
    public void start() {
        if (targets.isEmpty()) {
            // CREATED → SUCCEEDED directly. transitionTo stamps both start and end.
            transitionTo(StageExecution.State.SUCCEEDED);
            return;
        }
        if (transitionTo(StageExecution.State.RUNNING) == false) return;
        inFlight.set(targets.size());
        for (ShardTarget target : targets) {
            dispatchShardTask(target);
        }
    }

    private void dispatchShardTask(ShardTarget target) {
        FragmentExecutionRequest request = requestBuilder.apply(target);
        PendingExecutions pending = pendingFor(target);
        dispatcher.dispatchFragment(request, target.node(), new StreamingResponseListener<>() {
            @Override
            public void onStreamResponse(FragmentExecutionResponse response, boolean isLast) {
                config.searchExecutor().execute(() -> {
                    if (isDone()) return;

                    VectorSchemaRoot vsr = responseCodec.decode(response, config.bufferAllocator());
                    outputSink.feed(vsr);
                    metrics.addRowsProcessed(vsr.getRowCount());

                    if (isLast) {
                        metrics.incrementTasksCompleted();
                        onShardTerminated();
                    }
                });
            }

            @Override
            public void onFailure(Exception e) {
                captureFailure(new RuntimeException("Stage " + stage.getStageId() + " failed", e));
                metrics.incrementTasksFailed();
                onShardTerminated();
            }
        }, config.parentTask(), pending);
    }

    private void onShardTerminated() {
        if (inFlight.decrementAndGet() == 0) {
            Exception captured = getFailure();
            transitionTo(captured != null ? StageExecution.State.FAILED : StageExecution.State.SUCCEEDED);
        }
    }

    @Override
    public void cancel(String reason) {
        if (transitionTo(StageExecution.State.CANCELLED) == false) return;
        // Bridge to task framework: cancel the parent task so data nodes
        // see the cancellation via TaskCancellationService ban propagation.
        // AnalyticsQueryTask.shouldCancelChildrenOnCancellation() == true
        // ensures child shard tasks on data nodes are cancelled.
        org.opensearch.tasks.Task parentTask = config.parentTask();
        if (parentTask instanceof org.opensearch.tasks.CancellableTask ct && ct.isCancelled() == false) {
            ct.cancel(reason);
        }
    }

    @Override
    public ExchangeSink outputSink() {
        return outputSink;
    }

    @Override
    public ExchangeSource outputSource() {
        if (outputSink instanceof ExchangeSource source) {
            return source;
        }
        throw new UnsupportedOperationException("outputSink does not implement ExchangeSource");
    }

    private boolean isDone() {
        StageExecution.State s = getState();
        return s == StageExecution.State.SUCCEEDED || s == StageExecution.State.FAILED || s == StageExecution.State.CANCELLED;
    }

    private PendingExecutions pendingFor(ShardTarget target) {
        return pendingPerNode.computeIfAbsent(
            target.node().getId(),
            n -> new PendingExecutions(config.maxConcurrentShardRequests())
        );
    }

    // TODO: EVERYTHING BELOW THIS LINE SHOULD BE REMOVED WHEN WE HAVE VSR STREAMING

    /**
     * Converts a {@link FragmentExecutionResponse} (Java-native rows) to an Arrow
     * {@link VectorSchemaRoot}. Infers the Arrow type for each column from
     * the first non-null Java value in that column:
     * <ul>
     *   <li>{@code Long} → {@code BigInt}</li>
     *   <li>{@code Integer} → {@code Int(32, signed)}</li>
     *   <li>{@code Double} → {@code Float8 (FloatingPoint DOUBLE)}</li>
     *   <li>{@code Float} → {@code Float4 (FloatingPoint SINGLE)}</li>
     *   <li>{@code Boolean} → {@code Bit}</li>
     *   <li>{@code String} / {@code CharSequence} → {@code VarChar (Utf8)}</li>
     *   <li>{@code byte[]} → {@code VarBinary}</li>
     *   <li>all-null or unknown → {@code VarChar} fallback</li>
     * </ul>
     *
     * <p>The returned {@link VectorSchemaRoot} is owned by the caller (the
     * sink). Do <b>not</b> close it after feeding.
     *
     * @param response  the row-oriented shard response
     * @param allocator the buffer allocator for Arrow vectors
     * @return a new VectorSchemaRoot; caller owns and must close it
     */
    static VectorSchemaRoot scanResponseToArrow(FragmentExecutionResponse response, BufferAllocator allocator) {
        List<String> fieldNames = response.getFieldNames();
        List<Object[]> rows = response.getRows();

        if (allocator == null) {
            allocator = new RootAllocator();
        }

        // Infer Arrow type per column from the first non-null value
        List<Field> fields = new ArrayList<>();
        for (int col = 0; col < fieldNames.size(); col++) {
            ArrowType arrowType = inferArrowType(rows, col);
            fields.add(new Field(fieldNames.get(col), FieldType.nullable(arrowType), null));
        }
        Schema schema = new Schema(fields);

        VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, allocator);
        try {
            vsr.allocateNew();
            int rowCount = rows.size();
            for (int col = 0; col < fieldNames.size(); col++) {
                FieldVector vector = vsr.getVector(col);
                for (int r = 0; r < rowCount; r++) {
                    Object value = rows.get(r)[col];
                    setVectorValue(vector, r, value);
                }
                vector.setValueCount(rowCount);
            }
            vsr.setRowCount(rowCount);
            return vsr;
        } catch (Exception e) {
            vsr.close();
            throw e;
        }
    }

    /**
     * Infers the Arrow type for a column by scanning rows for the first
     * non-null value. Falls back to {@code Utf8} (VarChar) if all values
     * are null or the Java type is unrecognized.
     */
    private static ArrowType inferArrowType(List<Object[]> rows, int col) {
        for (Object[] row : rows) {
            Object value = row[col];
            if (value == null) continue;
            if (value instanceof Long) return new ArrowType.Int(64, true);
            if (value instanceof Integer) return new ArrowType.Int(32, true);
            if (value instanceof Short) return new ArrowType.Int(16, true);
            if (value instanceof Byte) return new ArrowType.Int(8, true);
            if (value instanceof Double) return new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
            if (value instanceof Float) return new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE);
            if (value instanceof Boolean) return ArrowType.Bool.INSTANCE;
            if (value instanceof CharSequence) return ArrowType.Utf8.INSTANCE;
            if (value instanceof byte[]) return ArrowType.Binary.INSTANCE;
            if (value instanceof Number) return new ArrowType.Int(64, true);
            // Unrecognized type — fall through to VarChar
            break;
        }
        return ArrowType.Utf8.INSTANCE;
    }

    /**
     * Sets a value on the appropriate Arrow vector type. Handles null by
     * calling {@code setNull}. For typed vectors, casts the Java value to
     * the expected type.
     */
    private static void setVectorValue(FieldVector vector, int index, Object value) {
        if (value == null) {
            vector.setNull(index);
            return;
        }
        if (vector instanceof BigIntVector) {
            ((BigIntVector) vector).setSafe(index, ((Number) value).longValue());
        } else if (vector instanceof IntVector) {
            ((IntVector) vector).setSafe(index, ((Number) value).intValue());
        } else if (vector instanceof SmallIntVector) {
            ((SmallIntVector) vector).setSafe(index, ((Number) value).shortValue());
        } else if (vector instanceof TinyIntVector) {
            ((TinyIntVector) vector).setSafe(index, ((Number) value).byteValue());
        } else if (vector instanceof Float8Vector) {
            ((Float8Vector) vector).setSafe(index, ((Number) value).doubleValue());
        } else if (vector instanceof Float4Vector) {
            ((Float4Vector) vector).setSafe(index, ((Number) value).floatValue());
        } else if (vector instanceof BitVector) {
            ((BitVector) vector).setSafe(index, ((Boolean) value) ? 1 : 0);
        } else if (vector instanceof VarCharVector) {
            ((VarCharVector) vector).setSafe(index, value.toString().getBytes(StandardCharsets.UTF_8));
        } else if (vector instanceof VarBinaryVector) {
            ((VarBinaryVector) vector).setSafe(index, (byte[]) value);
        } else {
            throw new IllegalArgumentException("Unsupported Arrow vector type: " + vector.getClass().getSimpleName());
        }
    }
}
