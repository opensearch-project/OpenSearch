/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.stream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.arrow.allocator.ArrowNativeAllocator;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Example: server-side handler producing native Arrow data.
 *
 * <p>Demonstrates the pipelined producer pattern:
 * <ol>
 *   <li>Receive an allocator sourced from the framework's FLIGHT pool</li>
 *   <li>For each batch, create a {@link VectorSchemaRoot}, populate it, and wrap it in a response</li>
 *   <li>Send via {@code sendResponseBatch()} — the framework zero-copy transfers
 *       the vectors into the Flight stream on the executor thread</li>
 *   <li>Call {@code completeStream()} when done</li>
 * </ol>
 *
 * <p><b>Known leak:</b> the action's allocator is not closed on plugin teardown
 * (the action is Guice-managed and not held by {@link StreamTransportExamplePlugin}).
 * On node shutdown, ArrowNativeAllocator.close() will warn about this outstanding child.
 * A clean fix would make this action {@link java.io.Closeable} and have the plugin track
 * and close it from {@code Plugin#close()}.
 */
public class TransportNativeArrowStreamDataAction extends TransportAction<NativeArrowStreamDataRequest, NativeArrowStreamDataResponse> {

    private static final String[] NAMES = { "Alice", "Bob", "Carol", "Dave", "Eve" };
    private final BufferAllocator allocator;

    @Inject
    public TransportNativeArrowStreamDataAction(
        StreamTransportService streamTransportService,
        ActionFilters actionFilters,
        ArrowNativeAllocator nativeAllocator
    ) {
        super(NativeArrowStreamDataAction.NAME, actionFilters, streamTransportService.getTaskManager());
        // Source the example plugin's allocator from the framework's FLIGHT pool. A streaming
        // transport plugin's Arrow buffers belong with the rest of arrow-flight-rpc's transport
        // accounting under POOL_FLIGHT, not as a sibling of the named pools.
        this.allocator = nativeAllocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_FLIGHT)
            .newChildAllocator("stream-transport-example", 0, Long.MAX_VALUE);
        streamTransportService.registerRequestHandler(
            NativeArrowStreamDataAction.NAME,
            ThreadPool.Names.GENERIC,
            NativeArrowStreamDataRequest::new,
            this::handleStreamRequest
        );
    }

    @Override
    protected void doExecute(Task task, NativeArrowStreamDataRequest request, ActionListener<NativeArrowStreamDataResponse> listener) {
        listener.onFailure(new UnsupportedOperationException("Use StreamTransportService"));
    }

    private void handleStreamRequest(NativeArrowStreamDataRequest request, TransportChannel channel, Task task) throws IOException {
        Schema schema = new Schema(
            List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null)
            )
        );

        try {
            for (int batch = 0; batch < request.getBatchCount(); batch++) {
                VectorSchemaRoot producerRoot = VectorSchemaRoot.create(schema, allocator);
                populateBatch(producerRoot, request.getRowsPerBatch(), batch);
                channel.sendResponseBatch(new NativeArrowStreamDataResponse(producerRoot));
            }
            channel.completeStream();
        } catch (StreamException e) {
            if (e.getErrorCode() != StreamErrorCode.CANCELLED) {
                channel.sendResponse(e);
            }
        } catch (Exception e) {
            channel.sendResponse(e);
        }
    }

    private void populateBatch(VectorSchemaRoot root, int rowCount, int batchIndex) {
        VarCharVector nameVector = (VarCharVector) root.getVector("name");
        IntVector ageVector = (IntVector) root.getVector("age");
        nameVector.allocateNew();
        ageVector.allocateNew();
        for (int i = 0; i < rowCount; i++) {
            nameVector.setSafe(i, NAMES[(batchIndex * rowCount + i) % NAMES.length].getBytes(StandardCharsets.UTF_8));
            ageVector.setSafe(i, 30 + i);
        }
        root.setRowCount(rowCount);
    }
}
