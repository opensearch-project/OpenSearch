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
import org.opensearch.arrow.flight.transport.ArrowFlightChannel;
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
 *   <li>Get the channel's allocator via {@link ArrowFlightChannel#from(TransportChannel)}</li>
 *   <li>For each batch, create a child allocator and a producer root</li>
 *   <li>Populate the root with typed vectors (VarChar, Int, etc.)</li>
 *   <li>Send via {@code sendResponseBatch()} — the framework does zero-copy transfer
 *       of the producer's buffers into the channel's shared root on the executor thread</li>
 *   <li>The producer root is consumed after transfer — don't reuse it</li>
 * </ol>
 *
 * <p>The producer can pipeline batches (queue them without waiting for flush)
 * because each batch has its own independent buffers. The executor drains them serially.
 */
public class TransportNativeArrowStreamDataAction extends TransportAction<NativeArrowStreamDataRequest, NativeArrowStreamDataResponse> {

    private static final String[] NAMES = { "Alice", "Bob", "Carol", "Dave", "Eve" };

    @Inject
    public TransportNativeArrowStreamDataAction(StreamTransportService streamTransportService, ActionFilters actionFilters) {
        super(NativeArrowStreamDataAction.NAME, actionFilters, streamTransportService.getTaskManager());
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
        // Get the channel's allocator. Use this directly for producer roots to ensure
        // same-allocator transfer (avoids Arrow's cross-allocator foreign buffer bug).
        BufferAllocator allocator = ArrowFlightChannel.from(channel).getAllocator();

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
