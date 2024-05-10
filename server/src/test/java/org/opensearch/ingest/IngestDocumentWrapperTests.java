/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest;

import org.opensearch.index.VersionType;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

public class IngestDocumentWrapperTests extends OpenSearchTestCase {

    private IngestDocument ingestDocument;

    private static final String INDEX = "index";
    private static final String ID = "id";
    private static final String ROUTING = "routing";
    private static final Long VERSION = 1L;
    private static final VersionType VERSION_TYPE = VersionType.INTERNAL;
    private static final String DOCUMENT_KEY = "foo";
    private static final String DOCUMENT_VALUE = "bar";
    private static final int SLOT = 12;

    @Before
    public void setup() throws Exception {
        super.setUp();
        Map<String, Object> document = new HashMap<>();
        document.put(DOCUMENT_KEY, DOCUMENT_VALUE);
        ingestDocument = new IngestDocument(INDEX, ID, ROUTING, VERSION, VERSION_TYPE, document);
    }

    public void testIngestDocumentWrapper() {
        Exception ex = new RuntimeException("runtime exception");
        IngestDocumentWrapper wrapper = new IngestDocumentWrapper(SLOT, ingestDocument, ex);
        assertEquals(wrapper.getSlot(), SLOT);
        assertEquals(wrapper.getException(), ex);
        assertEquals(wrapper.getIngestDocument(), ingestDocument);
    }
}
