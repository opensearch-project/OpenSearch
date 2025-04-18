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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

public class AbstractBatchingSystemProcessorTests extends OpenSearchTestCase {
    public void testSystemFactory_shouldNotModifyConfig() throws Exception {
        Map<String, Object> config = mock(Map.class);

        AbstractBatchingSystemProcessorTests.DummyProcessor.DummySystemProcessorFactory factory =
            new AbstractBatchingSystemProcessorTests.DummyProcessor.DummySystemProcessorFactory("DummyProcessor");
        factory.create(config);

        assertTrue(factory.isSystemGenerated());
        verifyNoInteractions(config);
    }

    static class DummyProcessor extends AbstractBatchingSystemProcessor {
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

        public static class DummySystemProcessorFactory extends AbstractBatchingSystemProcessor.Factory {
            protected DummySystemProcessorFactory(String processorType) {
                super(processorType);
            }

            @Override
            protected AbstractBatchingProcessor newProcessor(String tag, String description, Map<String, Object> config) {
                return new AbstractBatchingProcessorTests.DummyProcessor(1);
            }

            public AbstractBatchingProcessor create(Map<String, Object> config) throws Exception {
                return super.create(Collections.emptyMap(), "tag", "description", config);
            }
        }
    }
}
