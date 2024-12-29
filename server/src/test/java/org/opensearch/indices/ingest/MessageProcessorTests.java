/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.ingest;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
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

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MessageProcessorTests extends OpenSearchTestCase {
    private IngestionEngine ingestionEngine;
    private DocumentMapper documentMapper;
    private DocumentMapperForType documentMapperForType;
    private MessageProcessor processor;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        ingestionEngine = mock(IngestionEngine.class);
        documentMapperForType = mock(DocumentMapperForType.class);
        when(ingestionEngine.getDocumentMapperForType()).thenReturn(documentMapperForType);
        documentMapper = mock(DocumentMapper.class);
        when(documentMapperForType.getDocumentMapper()).thenReturn(documentMapper);
        processor = new MessageProcessor(ingestionEngine);
    }

    public void testGetOperation() {
        byte[] payload = "{\"name\":\"bob\", \"age\": 24}".getBytes();
        FakeIngestionSource.FakeIngestionShardPointer pointer = new FakeIngestionSource.FakeIngestionShardPointer(0);

        ParsedDocument parsedDocument = mock(ParsedDocument.class);
        when(documentMapper.parse(any())).thenReturn(parsedDocument);
        when(parsedDocument.docs()).thenReturn(List.of(new ParseContext.Document[]{new ParseContext.Document()}));

        Engine.Operation operation = processor.getOperation(payload, pointer);

        assertTrue(operation instanceof Engine.Index);
        ArgumentCaptor<SourceToParse> captor = ArgumentCaptor.forClass(SourceToParse.class);
        verify(documentMapper).parse(captor.capture());
        assertEquals("index", captor.getValue().index());
        assertEquals("null", captor.getValue().id());
    }
}
