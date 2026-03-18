/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.be.datafusion.action;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.arrow.flight.transport.FlightTransportChannel;
import org.opensearch.be.datafusion.jni.BatchCallback;
import org.opensearch.be.datafusion.jni.NativeBridge;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportService;

/**
 * Data-node handler: receives a SQL + parquet path, executes via Rust DataFusion,
 * and streams Arrow batches back via Flight native batch API.
 */
public class TransportPartialPlanAction extends HandledTransportAction<PartialPlanRequest, PartialPlanAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportPartialPlanAction.class);

    private final long runtimePtr;

    @Inject
    public TransportPartialPlanAction(
        TransportService transportService,
        StreamTransportService streamTransportService,
        ActionFilters actionFilters
    ) {
        super(PartialPlanAction.NAME, transportService, actionFilters, PartialPlanRequest::new);
        this.runtimePtr = NativeBridge.createRuntime();

        streamTransportService.registerRequestHandler(
            PartialPlanAction.NAME,
            ThreadPool.Names.SEARCH,
            PartialPlanRequest::new,
            this::handleStreamRequest
        );
        logger.info("Registered stream handler for {}", PartialPlanAction.NAME);
    }

    @Override
    protected void doExecute(Task task, PartialPlanRequest request, ActionListener<PartialPlanAction.Response> listener) {
        listener.onFailure(new UnsupportedOperationException("Use stream transport"));
    }

    private void handleStreamRequest(PartialPlanRequest request, TransportChannel channel, Task task) {
        FlightTransportChannel flightChannel = channel.get("flight", FlightTransportChannel.class)
            .orElseThrow(() -> new IllegalStateException("Expected FlightTransportChannel, got " + channel.getClass()));
        logger.info("Executing partial plan: sql=[{}], path=[{}]", request.getSql(), request.getParquetPath());
        BufferAllocator allocator = flightChannel.getAllocator();

        try {
            NativeBridge.executeAndStream(runtimePtr, request.getParquetPath(), request.getSql(), new BatchCallback() {
                @Override
                public void onBatch(long schemaAddr, long arrayAddr) {
                    try (ArrowSchema ffiSchema = ArrowSchema.wrap(schemaAddr);
                         ArrowArray ffiArray = ArrowArray.wrap(arrayAddr)) {
                        VectorSchemaRoot root = VectorSchemaRoot.create(
                            Data.importSchema(allocator, ffiSchema, null), allocator
                        );
                        Data.importIntoVectorSchemaRoot(allocator, ffiArray, root, null);
                        logger.debug("Streaming batch with {} rows", root.getRowCount());
                        flightChannel.sendNativeArrowBatch(root);
                    }
                }

                @Override
                public void onComplete() {
                    logger.info("Partial plan execution complete");
                    flightChannel.completeStream();
                }
            });
        } catch (Exception e) {
            logger.error("Error executing partial plan", e);
            try {
                flightChannel.sendResponse(e);
            } catch (Exception ex) {
                logger.error("Failed to send error response", ex);
            }
        }
    }
}
