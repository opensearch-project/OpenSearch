/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AbstractBatchProcessorTests extends OpenSearchTestCase {

    private static final String DESCRIPTION = "description";
    private static final String TAG = "tag";

    @Before
    public void setup() {}

    public void testBatchExecute_emptyInput() throws Exception {
        DummyProcessor processor = spy(new DummyProcessor(TAG, DESCRIPTION, 3));
        Consumer<List<IngestDocumentWrapper>> handler = (results) -> { assertTrue(results.isEmpty()); };
        processor.batchExecute(Collections.emptyList(), handler);
        verify(processor, never()).internalBatchExecute(anyList(), any());
    }

    public void testBatchExecute_singleBatchSize() throws Exception {
        DummyProcessor processor = spy(new DummyProcessor(TAG, DESCRIPTION, 3));
        List<IngestDocumentWrapper> wrapperList = Arrays.asList(
            IngestDocumentPreparer.createIngestDocumentWrapper(1),
            IngestDocumentPreparer.createIngestDocumentWrapper(2),
            IngestDocumentPreparer.createIngestDocumentWrapper(3)
        );
        List<IngestDocumentWrapper> resultList = new ArrayList<>();
        processor.batchExecute(wrapperList, resultList::addAll);
        assertEquals(wrapperList, resultList);
        verify(processor, times(1)).internalBatchExecute(anyList(), any());
    }

    public void testBatchExecute_multipleBatches() throws Exception {
        DummyProcessor processor = spy(new DummyProcessor(TAG, DESCRIPTION, 2));
        List<IngestDocumentWrapper> wrapperList = Arrays.asList(
            IngestDocumentPreparer.createIngestDocumentWrapper(1),
            IngestDocumentPreparer.createIngestDocumentWrapper(2),
            IngestDocumentPreparer.createIngestDocumentWrapper(3),
            IngestDocumentPreparer.createIngestDocumentWrapper(4),
            IngestDocumentPreparer.createIngestDocumentWrapper(5)
        );
        List<IngestDocumentWrapper> resultList = new ArrayList<>();
        processor.batchExecute(wrapperList, resultList::addAll);
        assertEquals(wrapperList, resultList);
        ArgumentCaptor<List> argumentCaptor = ArgumentCaptor.forClass(List.class);
        verify(processor, times(3)).internalBatchExecute(argumentCaptor.capture(), any());
        assertEquals(wrapperList.subList(0, 2), argumentCaptor.getAllValues().get(0));
        assertEquals(wrapperList.subList(2, 4), argumentCaptor.getAllValues().get(1));
        assertEquals(wrapperList.subList(4, 5), argumentCaptor.getAllValues().get(2));
    }

    public void testBatchExecute_randomBatches() throws Exception {
        int batchSize = randomIntBetween(2, 32);
        int docCount = randomIntBetween(2, 32);
        DummyProcessor processor = spy(new DummyProcessor(TAG, DESCRIPTION, batchSize));
        List<IngestDocumentWrapper> wrapperList = new ArrayList<>();
        for (int i = 0; i < docCount; ++i) {
            wrapperList.add(IngestDocumentPreparer.createIngestDocumentWrapper(i));
        }
        List<IngestDocumentWrapper> resultList = new ArrayList<>();
        processor.batchExecute(wrapperList, resultList::addAll);
        assertEquals(wrapperList, resultList);
        verify(processor, times(docCount / batchSize + (docCount % batchSize == 0 ? 0 : 1))).internalBatchExecute(
            anyList(),
            any()
        );
    }

    public void testBatchExecute_defaultBatchSize() throws Exception {
        DummyProcessor processor = spy(new DummyProcessor(TAG, DESCRIPTION, 1));
        List<IngestDocumentWrapper> wrapperList = Arrays.asList(
            IngestDocumentPreparer.createIngestDocumentWrapper(1),
            IngestDocumentPreparer.createIngestDocumentWrapper(2),
            IngestDocumentPreparer.createIngestDocumentWrapper(3)
        );
        List<IngestDocumentWrapper> resultList = new ArrayList<>();
        processor.batchExecute(wrapperList, resultList::addAll);
        for (int i = 0; i < wrapperList.size(); ++i) {
            assertEquals(wrapperList.get(i).getSlot(), resultList.get(i).getSlot());
            assertEquals(wrapperList.get(i).getIngestDocument(), resultList.get(i).getIngestDocument());
            assertEquals(wrapperList.get(i).getException(), resultList.get(i).getException());
        }
        verify(processor, never()).internalBatchExecute(anyList(), any());
        verify(processor, times(3)).execute(any(IngestDocument.class));
    }

    static class DummyProcessor extends AbstractBatchProcessor {

        protected DummyProcessor(String tag, String description, int batchSize) {
            super(tag, description, batchSize);
        }

        @Override
        public void internalBatchExecute(
            List<IngestDocumentWrapper> ingestDocumentWrappers,
            Consumer<List<IngestDocumentWrapper>> handler
        ) {
            handler.accept(ingestDocumentWrappers);
        }

        @Override
        public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
            return ingestDocument;
        }

        @Override
        public String getType() {
            return null;
        }
    }
}
