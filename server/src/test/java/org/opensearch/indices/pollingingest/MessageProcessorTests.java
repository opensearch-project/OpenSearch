/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.opensearch.index.Message;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.FakeIngestionSource;
import org.opensearch.index.engine.IngestionEngine;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.DocumentMapperForType;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
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
        processor = new MessageProcessorRunnable.MessageProcessor(ingestionEngine, "index");
    }

    public void testGetIndexOperation() throws IOException {
        byte[] payload = "{\"_id\":\"1\",\"_source\":{\"name\":\"bob\", \"age\": 24}}".getBytes(StandardCharsets.UTF_8);
        FakeIngestionSource.FakeIngestionShardPointer pointer = new FakeIngestionSource.FakeIngestionShardPointer(0);

        ParsedDocument parsedDocument = mock(ParsedDocument.class);
        when(documentMapper.parse(any())).thenReturn(parsedDocument);
        when(parsedDocument.rootDoc()).thenReturn(new ParseContext.Document());

        Engine.Operation operation = processor.getOperation(
            new ShardUpdateMessage(pointer, mock(Message.class), IngestionUtils.getParsedPayloadMap(payload), 0),
            MessageProcessorRunnable.MessageProcessorMetrics.create()
        );

        assertTrue(operation instanceof Engine.Index);
        ArgumentCaptor<SourceToParse> captor = ArgumentCaptor.forClass(SourceToParse.class);
        verify(documentMapper).parse(captor.capture());
        assertEquals("index", captor.getValue().index());
        assertEquals("1", captor.getValue().id());
    }

    public void testGetDeleteOperation() throws IOException {
        byte[] payload = "{\"_id\":\"1\",\"_op_type\":\"delete\"}".getBytes(StandardCharsets.UTF_8);
        FakeIngestionSource.FakeIngestionShardPointer pointer = new FakeIngestionSource.FakeIngestionShardPointer(0);

        Engine.Operation operation = processor.getOperation(
            new ShardUpdateMessage(pointer, mock(Message.class), IngestionUtils.getParsedPayloadMap(payload), -1),
            MessageProcessorRunnable.MessageProcessorMetrics.create()
        );

        assertTrue(operation instanceof Engine.Delete);
        Engine.Delete deleteOperation = (Engine.Delete) operation;
        assertEquals("1", deleteOperation.id());
    }

    public void testSkipNoSourceIndexOperation() throws IOException {
        byte[] payload = "{\"_id\":\"1\"}".getBytes(StandardCharsets.UTF_8);
        FakeIngestionSource.FakeIngestionShardPointer pointer = new FakeIngestionSource.FakeIngestionShardPointer(0);

        Engine.Operation operation = processor.getOperation(
            new ShardUpdateMessage(pointer, mock(Message.class), IngestionUtils.getParsedPayloadMap(payload), -1),
            MessageProcessorRunnable.MessageProcessorMetrics.create()
        );
        assertNull(operation);

        // source has wrong type
        payload = "{\"_id\":\"1\", \"_source\":1}".getBytes(StandardCharsets.UTF_8);

        operation = processor.getOperation(
            new ShardUpdateMessage(pointer, mock(Message.class), IngestionUtils.getParsedPayloadMap(payload), 0),
            MessageProcessorRunnable.MessageProcessorMetrics.create()
        );
        assertNull(operation);
    }

    public void testUnsupportedOperation() throws IOException {
        byte[] payload = "{\"_id\":\"1\", \"_op_tpe\":\"update\"}".getBytes(StandardCharsets.UTF_8);
        FakeIngestionSource.FakeIngestionShardPointer pointer = new FakeIngestionSource.FakeIngestionShardPointer(0);

        Engine.Operation operation = processor.getOperation(
            new ShardUpdateMessage(pointer, mock(Message.class), IngestionUtils.getParsedPayloadMap(payload), -1),
            MessageProcessorRunnable.MessageProcessorMetrics.create()
        );
        assertNull(operation);
    }

    public void testMissingID() throws IOException {
        byte[] payload = "{\"_source\":{\"name\":\"bob\", \"age\": 24}}".getBytes(StandardCharsets.UTF_8);
        FakeIngestionSource.FakeIngestionShardPointer pointer = new FakeIngestionSource.FakeIngestionShardPointer(0);

        ParsedDocument parsedDocument = mock(ParsedDocument.class);
        when(documentMapper.parse(any())).thenReturn(parsedDocument);
        when(parsedDocument.rootDoc()).thenReturn(new ParseContext.Document());
        Engine.Operation operation = processor.getOperation(
            new ShardUpdateMessage(pointer, mock(Message.class), IngestionUtils.getParsedPayloadMap(payload), -1),
            MessageProcessorRunnable.MessageProcessorMetrics.create()
        );
        assertNull(operation);
    }

    public void testDeleteWithAutoGeneratedID() throws IOException {
        byte[] payload = "{\"_id\":\"1\",\"_op_type\":\"delete\"}".getBytes(StandardCharsets.UTF_8);
        FakeIngestionSource.FakeIngestionShardPointer pointer = new FakeIngestionSource.FakeIngestionShardPointer(0);

        ParsedDocument parsedDocument = mock(ParsedDocument.class);
        when(documentMapper.parse(any())).thenReturn(parsedDocument);
        when(parsedDocument.rootDoc()).thenReturn(new ParseContext.Document());
        Engine.Operation operation = processor.getOperation(
            new ShardUpdateMessage(pointer, mock(Message.class), IngestionUtils.getParsedPayloadMap(payload), System.currentTimeMillis()),
            MessageProcessorRunnable.MessageProcessorMetrics.create()
        );
        assertTrue(operation instanceof Engine.NoOp);
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
}
