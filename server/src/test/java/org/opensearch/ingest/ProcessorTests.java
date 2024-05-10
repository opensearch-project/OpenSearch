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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.opensearch.ingest.IngestDocumentPreparer.SHOULD_FAIL_KEY;

public class ProcessorTests extends OpenSearchTestCase {
    private Processor processor;
    private static final String FIELD_KEY = "result";
    private static final String FIELD_VALUE_PROCESSED = "processed";

    @Before
    public void setup() {}

    public void test_batchExecute_success() {
        processor = new FakeProcessor("type", "tag", "description", doc -> { doc.setFieldValue(FIELD_KEY, FIELD_VALUE_PROCESSED); });
        List<IngestDocumentWrapper> wrapperList = Arrays.asList(
            IngestDocumentPreparer.createIngestDocumentWrapper(1),
            IngestDocumentPreparer.createIngestDocumentWrapper(2),
            IngestDocumentPreparer.createIngestDocumentWrapper(3)
        );
        processor.batchExecute(wrapperList, results -> {
            assertEquals(3, results.size());
            for (IngestDocumentWrapper wrapper : results) {
                assertNull(wrapper.getException());
                assertEquals(FIELD_VALUE_PROCESSED, wrapper.getIngestDocument().getFieldValue(FIELD_KEY, String.class));
            }
        });
    }

    public void test_batchExecute_empty() {
        processor = new FakeProcessor("type", "tag", "description", doc -> { doc.setFieldValue(FIELD_KEY, FIELD_VALUE_PROCESSED); });
        processor.batchExecute(Collections.emptyList(), results -> { assertEquals(0, results.size()); });
    }

    public void test_batchExecute_exception() {
        processor = new FakeProcessor("type", "tag", "description", doc -> {
            if (doc.hasField(SHOULD_FAIL_KEY) && doc.getFieldValue(SHOULD_FAIL_KEY, Boolean.class)) {
                throw new RuntimeException("fail");
            }
            doc.setFieldValue(FIELD_KEY, FIELD_VALUE_PROCESSED);
        });
        List<IngestDocumentWrapper> wrapperList = Arrays.asList(
            IngestDocumentPreparer.createIngestDocumentWrapper(1),
            IngestDocumentPreparer.createIngestDocumentWrapper(2, true),
            IngestDocumentPreparer.createIngestDocumentWrapper(3)
        );
        processor.batchExecute(wrapperList, results -> {
            assertEquals(3, results.size());
            for (IngestDocumentWrapper wrapper : results) {
                if (wrapper.getSlot() == 2) {
                    assertNotNull(wrapper.getException());
                    assertNull(wrapper.getIngestDocument());
                } else {
                    assertNull(wrapper.getException());
                    assertEquals(FIELD_VALUE_PROCESSED, wrapper.getIngestDocument().getFieldValue(FIELD_KEY, String.class));
                }
            }
        });
    }
}
