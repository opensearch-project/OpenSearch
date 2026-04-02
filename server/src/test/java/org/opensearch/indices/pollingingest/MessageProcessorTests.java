/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.Message;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.FakeIngestionSource;
import org.opensearch.index.engine.IngestionEngine;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.DocumentMapperForType;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.ingest.IngestService;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.IntConsumer;

import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MessageProcessorTests extends OpenSearchTestCase {
    private IngestionEngine ingestionEngine;
    private DocumentMapper documentMapper;
    private DocumentMapperForType documentMapperForType;
    private MessageProcessorRunnable.MessageProcessor processor;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        ingestionEngine = mock(IngestionEngine.class);
        documentMapperForType = mock(DocumentMapperForType.class);
        when(ingestionEngine.getDocumentMapperForType()).thenReturn(documentMapperForType);

        documentMapper = mock(DocumentMapper.class);
        when(documentMapperForType.getDocumentMapper()).thenReturn(documentMapper);
        processor = new MessageProcessorRunnable.MessageProcessor(
            ingestionEngine,
            "index",
            new IngestPipelineExecutor(mock(IngestService.class), "index", (String) null)
        );
    }

    public void testGetIndexOperation() throws IOException {
        byte[] payload = "{\"_id\":\"1\",\"_source\":{\"name\":\"bob\", \"age\": 24}}".getBytes(StandardCharsets.UTF_8);
        FakeIngestionSource.FakeIngestionShardPointer pointer = new FakeIngestionSource.FakeIngestionShardPointer(0);

        ParsedDocument parsedDocument = mock(ParsedDocument.class);
        when(documentMapper.parse(any())).thenReturn(parsedDocument);
        when(parsedDocument.rootDoc()).thenReturn(new ParseContext.Document());

        MessageProcessorRunnable.MessageOperation operation = processor.getOperation(
            new ShardUpdateMessage(pointer, mock(Message.class), IngestionUtils.getParsedPayloadMap(payload), 0),
            MessageProcessorRunnable.MessageProcessorMetrics.create()
        );

        assertTrue(operation.engineOperation() instanceof Engine.Index);
        assertEquals(DocWriteRequest.OpType.INDEX, operation.opType());
        ArgumentCaptor<SourceToParse> captor = ArgumentCaptor.forClass(SourceToParse.class);
        verify(documentMapper).parse(captor.capture());
        assertEquals("index", captor.getValue().index());
        assertEquals("1", captor.getValue().id());
    }

    public void testGetIndexOperationInCreateMode() throws IOException {
        byte[] payload = "{\"_id\":\"1\",\"_source\":{\"name\":\"bob\", \"age\": 24}, \"_op_type\": \"create\"}".getBytes(
            StandardCharsets.UTF_8
        );
        FakeIngestionSource.FakeIngestionShardPointer pointer = new FakeIngestionSource.FakeIngestionShardPointer(0);

        ParsedDocument parsedDocument = mock(ParsedDocument.class);
        when(documentMapper.parse(any())).thenReturn(parsedDocument);
        when(parsedDocument.rootDoc()).thenReturn(new ParseContext.Document());

        MessageProcessorRunnable.MessageOperation operation = processor.getOperation(
            new ShardUpdateMessage(pointer, mock(Message.class), IngestionUtils.getParsedPayloadMap(payload), 0),
            MessageProcessorRunnable.MessageProcessorMetrics.create()
        );

        assertTrue(operation.engineOperation() instanceof Engine.Index);
        assertEquals(DocWriteRequest.OpType.CREATE, operation.opType());
        ArgumentCaptor<SourceToParse> captor = ArgumentCaptor.forClass(SourceToParse.class);
        verify(documentMapper).parse(captor.capture());
        assertEquals("index", captor.getValue().index());
        assertEquals("1", captor.getValue().id());
    }

    public void testGetDeleteOperation() throws IOException {
        byte[] payload = "{\"_id\":\"1\",\"_op_type\":\"delete\"}".getBytes(StandardCharsets.UTF_8);
        FakeIngestionSource.FakeIngestionShardPointer pointer = new FakeIngestionSource.FakeIngestionShardPointer(0);

        MessageProcessorRunnable.MessageOperation operation = processor.getOperation(
            new ShardUpdateMessage(pointer, mock(Message.class), IngestionUtils.getParsedPayloadMap(payload), -1),
            MessageProcessorRunnable.MessageProcessorMetrics.create()
        );

        assertTrue(operation.engineOperation() instanceof Engine.Delete);
        Engine.Delete deleteOperation = (Engine.Delete) operation.engineOperation();
        assertEquals("1", deleteOperation.id());
    }

    public void testSkipNoSourceIndexOperation() throws IOException {
        final byte[] payload = "{\"_id\":\"1\"}".getBytes(StandardCharsets.UTF_8);
        FakeIngestionSource.FakeIngestionShardPointer pointer = new FakeIngestionSource.FakeIngestionShardPointer(0);

        assertThrows(
            IllegalArgumentException.class,
            () -> processor.getOperation(
                new ShardUpdateMessage(pointer, mock(Message.class), IngestionUtils.getParsedPayloadMap(payload), -1),
                MessageProcessorRunnable.MessageProcessorMetrics.create()
            )
        );

        // source has wrong type
        final byte[] payload2 = "{\"_id\":\"1\", \"_source\":1}".getBytes(StandardCharsets.UTF_8);
        assertThrows(
            IllegalArgumentException.class,
            () -> processor.getOperation(
                new ShardUpdateMessage(pointer, mock(Message.class), IngestionUtils.getParsedPayloadMap(payload2), 0),
                MessageProcessorRunnable.MessageProcessorMetrics.create()
            )
        );
    }

    public void testUnsupportedOperation() throws IOException {
        byte[] payload = "{\"_id\":\"1\", \"_op_type\":\"update\"}".getBytes(StandardCharsets.UTF_8);
        FakeIngestionSource.FakeIngestionShardPointer pointer = new FakeIngestionSource.FakeIngestionShardPointer(0);

        assertThrows(
            IllegalArgumentException.class,
            () -> processor.getOperation(
                new ShardUpdateMessage(pointer, mock(Message.class), IngestionUtils.getParsedPayloadMap(payload), -1),
                MessageProcessorRunnable.MessageProcessorMetrics.create()
            )
        );
    }

    public void testInvalidOperationType() throws IOException {
        byte[] payload = "{\"_id\":\"1\", \"_op_type\":100}".getBytes(StandardCharsets.UTF_8);
        FakeIngestionSource.FakeIngestionShardPointer pointer = new FakeIngestionSource.FakeIngestionShardPointer(0);

        assertThrows(
            IllegalArgumentException.class,
            () -> processor.getOperation(
                new ShardUpdateMessage(pointer, mock(Message.class), IngestionUtils.getParsedPayloadMap(payload), -1),
                MessageProcessorRunnable.MessageProcessorMetrics.create()
            )
        );
    }

    public void testMissingID() throws IOException {
        byte[] payload = "{\"_source\":{\"name\":\"bob\", \"age\": 24}}".getBytes(StandardCharsets.UTF_8);
        FakeIngestionSource.FakeIngestionShardPointer pointer = new FakeIngestionSource.FakeIngestionShardPointer(0);
        ParsedDocument parsedDocument = mock(ParsedDocument.class);
        when(documentMapper.parse(any())).thenReturn(parsedDocument);
        when(parsedDocument.rootDoc()).thenReturn(new ParseContext.Document());

        assertThrows(
            IllegalArgumentException.class,
            () -> processor.getOperation(
                new ShardUpdateMessage(pointer, mock(Message.class), IngestionUtils.getParsedPayloadMap(payload), -1),
                MessageProcessorRunnable.MessageProcessorMetrics.create()
            )
        );
    }

    public void testDeleteWithAutoGeneratedID() throws IOException {
        byte[] payload = "{\"_id\":\"1\",\"_op_type\":\"delete\"}".getBytes(StandardCharsets.UTF_8);
        FakeIngestionSource.FakeIngestionShardPointer pointer = new FakeIngestionSource.FakeIngestionShardPointer(0);

        ParsedDocument parsedDocument = mock(ParsedDocument.class);
        when(documentMapper.parse(any())).thenReturn(parsedDocument);
        when(parsedDocument.rootDoc()).thenReturn(new ParseContext.Document());
        MessageProcessorRunnable.MessageOperation operation = processor.getOperation(
            new ShardUpdateMessage(pointer, mock(Message.class), IngestionUtils.getParsedPayloadMap(payload), System.currentTimeMillis()),
            MessageProcessorRunnable.MessageProcessorMetrics.create()
        );
        assertTrue(operation.engineOperation() instanceof Engine.NoOp);
    }

    public void testMessageProcessorMetrics() {
        MessageProcessorRunnable.MessageProcessorMetrics metrics1 = MessageProcessorRunnable.MessageProcessorMetrics.create();
        metrics1.processedCounter().inc(100);
        metrics1.invalidMessageCounter().inc(5);
        metrics1.versionConflictCounter().inc(2);
        metrics1.failedMessageCounter().inc(1);
        metrics1.failedMessageDroppedCounter().inc(1);
        metrics1.processorThreadInterruptCounter().inc(0);

        MessageProcessorRunnable.MessageProcessorMetrics metrics2 = MessageProcessorRunnable.MessageProcessorMetrics.create();
        metrics2.processedCounter().inc(100);
        metrics2.invalidMessageCounter().inc(0);
        metrics2.versionConflictCounter().inc(0);
        metrics2.failedMessageCounter().inc(100);
        metrics2.failedMessageDroppedCounter().inc(100);
        metrics2.processorThreadInterruptCounter().inc(1);

        MessageProcessorRunnable.MessageProcessorMetrics combinedMetric = metrics1.combine(metrics2);
        assertEquals(200, combinedMetric.processedCounter().count());
        assertEquals(5, combinedMetric.invalidMessageCounter().count());
        assertEquals(2, combinedMetric.versionConflictCounter().count());
        assertEquals(101, combinedMetric.failedMessageCounter().count());
        assertEquals(101, combinedMetric.failedMessageDroppedCounter().count());
        assertEquals(1, combinedMetric.processorThreadInterruptCounter().count());
    }

    public void testMessageRetrySuccess() throws Exception {
        MessageProcessorRunnable.MessageProcessor processor = mock(MessageProcessorRunnable.MessageProcessor.class);
        DropIngestionErrorStrategy errorStrategy = new DropIngestionErrorStrategy("ingestion_source");
        MessageProcessorRunnable messageProcessorRunnable = new MessageProcessorRunnable(
            new ArrayBlockingQueue<>(5),
            processor,
            errorStrategy,
            "test_index",
            0
        );
        messageProcessorRunnable.getBlockingQueue().put(new ShardUpdateMessage(null, null, null, 0));

        doThrow(new RuntimeException()).doNothing().when(processor).process(any(), any());

        Thread thread = new Thread(messageProcessorRunnable::run);
        thread.start();
        assertBusy(() -> {
            verify(processor, times(2)).process(any(), any());
            assertEquals(0, messageProcessorRunnable.getMessageProcessorMetrics().failedMessageDroppedCounter().count());
            assertEquals(1, messageProcessorRunnable.getMessageProcessorMetrics().failedMessageCounter().count());
        }, 1, TimeUnit.MINUTES);

        messageProcessorRunnable.close();
        thread.interrupt();
    }

    public void testDropPolicyMessageRetryFail() throws Exception {
        MessageProcessorRunnable.MessageProcessor processor = mock(MessageProcessorRunnable.MessageProcessor.class);
        DropIngestionErrorStrategy errorStrategy = new DropIngestionErrorStrategy("ingestion_source");
        MessageProcessorRunnable messageProcessorRunnable = new MessageProcessorRunnable(
            new ArrayBlockingQueue<>(5),
            processor,
            errorStrategy,
            "test_index",
            0
        );
        messageProcessorRunnable.getBlockingQueue()
            .put(new ShardUpdateMessage(mock(IngestionShardPointer.class), null, Collections.emptyMap(), 0));
        messageProcessorRunnable.getBlockingQueue()
            .put(new ShardUpdateMessage(mock(IngestionShardPointer.class), null, Collections.emptyMap(), -1));

        doThrow(new RuntimeException()).when(processor).process(any(), any());

        Thread thread = new Thread(messageProcessorRunnable::run);
        thread.start();
        assertBusy(() -> {
            verify(processor, times(6)).process(any(), any());
            assertEquals(2, messageProcessorRunnable.getMessageProcessorMetrics().failedMessageDroppedCounter().count());
            assertEquals(6, messageProcessorRunnable.getMessageProcessorMetrics().failedMessageCounter().count());
        }, 2, TimeUnit.MINUTES);

        messageProcessorRunnable.close();
        thread.interrupt();
    }

    // --- Pipeline execution tests ---

    /**
     * Creates a MessageProcessor with a mocked IngestService and index settings that have a final_pipeline configured.
     */
    private MessageProcessorRunnable.MessageProcessor createProcessorWithPipeline(IngestService ingestService, String finalPipeline)
        throws Exception {
        IngestionEngine engine = mock(IngestionEngine.class);
        DocumentMapperForType dmft = mock(DocumentMapperForType.class);
        DocumentMapper dm = mock(DocumentMapper.class);
        when(engine.getDocumentMapperForType()).thenReturn(dmft);
        when(dmft.getDocumentMapper()).thenReturn(dm);

        ParsedDocument parsedDoc = mock(ParsedDocument.class);
        when(parsedDoc.rootDoc()).thenReturn(new ParseContext.Document());
        when(dm.parse(any())).thenReturn(parsedDoc);

        // Use IngestPipelineExecutor with pre-resolved pipeline name
        String resolvedPipeline = "_none".equals(finalPipeline) ? null : finalPipeline;
        IngestPipelineExecutor pipelineExecutor = new IngestPipelineExecutor(ingestService, "test_index", resolvedPipeline);
        return new MessageProcessorRunnable.MessageProcessor(engine, "test_index", pipelineExecutor);
    }

    /**
     * Mocks IngestService.executeBulkRequest to simulate successful pipeline execution
     * that modifies the document source (adds a field).
     */
    private void mockPipelineExecution(IngestService ingestService, String addedField, Object addedValue) {
        doAnswer(invocation -> {
            Iterable<DocWriteRequest<?>> requests = invocation.getArgument(1);
            BiConsumer<Thread, Exception> onCompletion = invocation.getArgument(3);

            // Simulate pipeline adding a field to the source
            for (DocWriteRequest<?> req : requests) {
                IndexRequest indexRequest = (IndexRequest) req;
                java.util.Map<String, Object> sourceMap = indexRequest.sourceAsMap();
                sourceMap.put(addedField, addedValue);
                indexRequest.source(sourceMap);
            }

            onCompletion.accept(Thread.currentThread(), null);
            return null;
        }).when(ingestService).executeBulkRequest(anyInt(), any(), any(), any(), any(), anyString());
    }

    public void testProcessWithFinalPipeline() throws Exception {
        IngestService ingestService = mock(IngestService.class);
        mockPipelineExecution(ingestService, "pipeline_processed", true);

        MessageProcessorRunnable.MessageProcessor proc = createProcessorWithPipeline(ingestService, "test-pipeline");

        byte[] payload = "{\"_id\":\"1\",\"_source\":{\"name\":\"alice\"}}".getBytes(StandardCharsets.UTF_8);
        FakeIngestionSource.FakeIngestionShardPointer pointer = new FakeIngestionSource.FakeIngestionShardPointer(0);

        MessageProcessorRunnable.MessageOperation operation = proc.getOperation(
            new ShardUpdateMessage(pointer, mock(Message.class), IngestionUtils.getParsedPayloadMap(payload), -1),
            MessageProcessorRunnable.MessageProcessorMetrics.create()
        );

        assertTrue(operation.engineOperation() instanceof Engine.Index);

        // Verify IngestService was called
        verify(ingestService).executeBulkRequest(anyInt(), any(), any(), any(), any(), anyString());
    }

    public void testProcessWithNoPipelines() throws Exception {
        IngestService ingestService = mock(IngestService.class);

        // final_pipeline = _none → no pipeline configured
        MessageProcessorRunnable.MessageProcessor proc = createProcessorWithPipeline(ingestService, "_none");

        byte[] payload = "{\"_id\":\"1\",\"_source\":{\"name\":\"alice\"}}".getBytes(StandardCharsets.UTF_8);
        FakeIngestionSource.FakeIngestionShardPointer pointer = new FakeIngestionSource.FakeIngestionShardPointer(0);

        MessageProcessorRunnable.MessageOperation operation = proc.getOperation(
            new ShardUpdateMessage(pointer, mock(Message.class), IngestionUtils.getParsedPayloadMap(payload), -1),
            MessageProcessorRunnable.MessageProcessorMetrics.create()
        );

        assertTrue(operation.engineOperation() instanceof Engine.Index);

        // IngestService should NOT be called when no pipelines configured
        verify(ingestService, never()).executeBulkRequest(anyInt(), any(), any(), any(), any(), anyString());
    }

    public void testPipelineDropsDocument() throws Exception {
        IngestService ingestService = mock(IngestService.class);

        // Mock pipeline that drops the document
        doAnswer(invocation -> {
            IntConsumer onDropped = invocation.getArgument(4);
            BiConsumer<Thread, Exception> onCompletion = invocation.getArgument(3);
            onDropped.accept(0);
            onCompletion.accept(Thread.currentThread(), null);
            return null;
        }).when(ingestService).executeBulkRequest(anyInt(), any(), any(), any(), any(), anyString());

        MessageProcessorRunnable.MessageProcessor proc = createProcessorWithPipeline(ingestService, "drop-pipeline");

        byte[] payload = "{\"_id\":\"1\",\"_source\":{\"name\":\"alice\"}}".getBytes(StandardCharsets.UTF_8);
        FakeIngestionSource.FakeIngestionShardPointer pointer = new FakeIngestionSource.FakeIngestionShardPointer(0);

        MessageProcessorRunnable.MessageOperation operation = proc.getOperation(
            new ShardUpdateMessage(pointer, mock(Message.class), IngestionUtils.getParsedPayloadMap(payload), -1),
            MessageProcessorRunnable.MessageProcessorMetrics.create()
        );

        // Dropped documents should return NoOp
        assertTrue(operation.engineOperation() instanceof Engine.NoOp);
    }

    public void testPipelineMutatesId_Throws() throws Exception {
        IngestService ingestService = mock(IngestService.class);

        // Mock pipeline that changes _id
        doAnswer(invocation -> {
            Iterable<DocWriteRequest<?>> requests = invocation.getArgument(1);
            BiConsumer<Thread, Exception> onCompletion = invocation.getArgument(3);
            for (DocWriteRequest<?> req : requests) {
                ((IndexRequest) req).id("changed_id");
            }
            onCompletion.accept(Thread.currentThread(), null);
            return null;
        }).when(ingestService).executeBulkRequest(anyInt(), any(), any(), any(), any(), anyString());

        MessageProcessorRunnable.MessageProcessor proc = createProcessorWithPipeline(ingestService, "mutate-id-pipeline");

        byte[] payload = "{\"_id\":\"1\",\"_source\":{\"name\":\"alice\"}}".getBytes(StandardCharsets.UTF_8);
        FakeIngestionSource.FakeIngestionShardPointer pointer = new FakeIngestionSource.FakeIngestionShardPointer(0);

        IllegalStateException e = assertThrows(
            IllegalStateException.class,
            () -> proc.getOperation(
                new ShardUpdateMessage(pointer, mock(Message.class), IngestionUtils.getParsedPayloadMap(payload), -1),
                MessageProcessorRunnable.MessageProcessorMetrics.create()
            )
        );
        assertTrue(e.getMessage().contains("_id mutations are not allowed"));
    }

    public void testPipelineFailure() throws Exception {
        IngestService ingestService = mock(IngestService.class);

        // Mock pipeline that fails
        doAnswer(invocation -> {
            BiConsumer<Integer, Exception> onFailure = invocation.getArgument(2);
            BiConsumer<Thread, Exception> onCompletion = invocation.getArgument(3);
            onFailure.accept(0, new RuntimeException("Pipeline processor failed"));
            onCompletion.accept(Thread.currentThread(), null);
            return null;
        }).when(ingestService).executeBulkRequest(anyInt(), any(), any(), any(), any(), anyString());

        MessageProcessorRunnable.MessageProcessor proc = createProcessorWithPipeline(ingestService, "fail-pipeline");

        byte[] payload = "{\"_id\":\"1\",\"_source\":{\"name\":\"alice\"}}".getBytes(StandardCharsets.UTF_8);
        FakeIngestionSource.FakeIngestionShardPointer pointer = new FakeIngestionSource.FakeIngestionShardPointer(0);

        RuntimeException e = assertThrows(
            RuntimeException.class,
            () -> proc.getOperation(
                new ShardUpdateMessage(pointer, mock(Message.class), IngestionUtils.getParsedPayloadMap(payload), -1),
                MessageProcessorRunnable.MessageProcessorMetrics.create()
            )
        );
        assertTrue(e.getCause().getMessage().contains("Ingest pipeline execution failed"));
    }

    public void testPipelineNotCalledForDeleteOperations() throws Exception {
        IngestService ingestService = mock(IngestService.class);

        MessageProcessorRunnable.MessageProcessor proc = createProcessorWithPipeline(ingestService, "test-pipeline");

        byte[] payload = "{\"_id\":\"1\",\"_op_type\":\"delete\"}".getBytes(StandardCharsets.UTF_8);
        FakeIngestionSource.FakeIngestionShardPointer pointer = new FakeIngestionSource.FakeIngestionShardPointer(0);

        MessageProcessorRunnable.MessageOperation operation = proc.getOperation(
            new ShardUpdateMessage(pointer, mock(Message.class), IngestionUtils.getParsedPayloadMap(payload), -1),
            MessageProcessorRunnable.MessageProcessorMetrics.create()
        );

        assertTrue(operation.engineOperation() instanceof Engine.Delete);

        // Pipeline should NOT be called for delete operations
        verify(ingestService, never()).executeBulkRequest(anyInt(), any(), any(), any(), any(), anyString());
    }
}
