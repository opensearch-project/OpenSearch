/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public class AbstractBatchingProcessorTests extends OpenSearchTestCase {

    public void testBatchExecute_emptyInput() {
        DummyProcessor processor = new DummyProcessor(3);
        Consumer<List<IngestDocumentWrapper>> handler = (results) -> assertTrue(results.isEmpty());
        processor.batchExecute(Collections.emptyList(), handler);
        assertTrue(processor.getSubBatches().isEmpty());
    }

    public void testBatchExecute_singleBatchSize() {
        DummyProcessor processor = new DummyProcessor(3);
        List<IngestDocumentWrapper> wrapperList = Arrays.asList(
            IngestDocumentPreparer.createIngestDocumentWrapper(1),
            IngestDocumentPreparer.createIngestDocumentWrapper(2),
            IngestDocumentPreparer.createIngestDocumentWrapper(3)
        );
        List<IngestDocumentWrapper> resultList = new ArrayList<>();
        processor.batchExecute(wrapperList, resultList::addAll);
        assertEquals(wrapperList, resultList);
        assertEquals(1, processor.getSubBatches().size());
        assertEquals(wrapperList, processor.getSubBatches().get(0));
    }

    public void testBatchExecute_multipleBatches() {
        DummyProcessor processor = new DummyProcessor(2);
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
        assertEquals(3, processor.getSubBatches().size());
        assertEquals(wrapperList.subList(0, 2), processor.getSubBatches().get(0));
        assertEquals(wrapperList.subList(2, 4), processor.getSubBatches().get(1));
        assertEquals(wrapperList.subList(4, 5), processor.getSubBatches().get(2));
    }

    public void testBatchExecute_randomBatches() {
        int batchSize = randomIntBetween(2, 32);
        int docCount = randomIntBetween(2, 32);
        DummyProcessor processor = new DummyProcessor(batchSize);
        List<IngestDocumentWrapper> wrapperList = new ArrayList<>();
        for (int i = 0; i < docCount; ++i) {
            wrapperList.add(IngestDocumentPreparer.createIngestDocumentWrapper(i));
        }
        List<IngestDocumentWrapper> resultList = new ArrayList<>();
        processor.batchExecute(wrapperList, resultList::addAll);
        assertEquals(wrapperList, resultList);
        assertEquals(docCount / batchSize + (docCount % batchSize == 0 ? 0 : 1), processor.getSubBatches().size());
    }

    public void testBatchExecute_defaultBatchSize() {
        DummyProcessor processor = new DummyProcessor(1);
        List<IngestDocumentWrapper> wrapperList = Arrays.asList(
            IngestDocumentPreparer.createIngestDocumentWrapper(1),
            IngestDocumentPreparer.createIngestDocumentWrapper(2),
            IngestDocumentPreparer.createIngestDocumentWrapper(3)
        );
        List<IngestDocumentWrapper> resultList = new ArrayList<>();
        processor.batchExecute(wrapperList, resultList::addAll);
        assertEquals(wrapperList, resultList);
        assertEquals(3, processor.getSubBatches().size());
        assertEquals(wrapperList.subList(0, 1), processor.getSubBatches().get(0));
        assertEquals(wrapperList.subList(1, 2), processor.getSubBatches().get(1));
        assertEquals(wrapperList.subList(2, 3), processor.getSubBatches().get(2));
    }

    static class DummyProcessor extends AbstractBatchingProcessor {
        private List<List<IngestDocumentWrapper>> subBatches = new ArrayList<>();

        public List<List<IngestDocumentWrapper>> getSubBatches() {
            return subBatches;
        }

        protected DummyProcessor(int batchSize) {
            super("tag", "description", batchSize);
        }

        @Override
        public void subBatchExecute(List<IngestDocumentWrapper> ingestDocumentWrappers, Consumer<List<IngestDocumentWrapper>> handler) {
            subBatches.add(ingestDocumentWrappers);
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
