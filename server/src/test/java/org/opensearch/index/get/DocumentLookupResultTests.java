/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.get;

import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class DocumentLookupResultTests extends OpenSearchTestCase {

    public void testNotFound() {
        DocumentLookupResult r = DocumentLookupResult.notFound("x");
        assertFalse(r.exists());
        assertEquals("x", r.id());
        assertEquals(Versions.NOT_FOUND, r.version());
        assertEquals(SequenceNumbers.UNASSIGNED_SEQ_NO, r.seqNo());
        assertEquals(SequenceNumbers.UNASSIGNED_PRIMARY_TERM, r.primaryTerm());
        assertNull(r.source());
        assertTrue(r.documentFields().isEmpty());
        assertTrue(r.metadataFields().isEmpty());
    }

    public void testNullFieldMapsNormalizedToEmpty() {
        DocumentLookupResult r = new DocumentLookupResult("1", 1L, true, null, 0L, 1L, null, null);
        assertNotNull(r.documentFields());
        assertNotNull(r.metadataFields());
        assertTrue(r.documentFields().isEmpty());
        assertTrue(r.metadataFields().isEmpty());
    }

    public void testEqualsAndHashCode() {
        BytesReference src = new BytesArray("{\"f\":1}");
        DocumentLookupResult a = new DocumentLookupResult("1", 2L, true, src, 3L, 1L, Map.of(), Map.of());
        DocumentLookupResult b = new DocumentLookupResult("1", 2L, true, new BytesArray("{\"f\":1}"), 3L, 1L, Map.of(), Map.of());
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        DocumentLookupResult differentVersion = new DocumentLookupResult("1", 9L, true, src, 3L, 1L, Map.of(), Map.of());
        assertNotEquals(a, differentVersion);
    }

    public void testPreMaterializedShape() {
        DocumentLookupResult r = new DocumentLookupResult("1", 5L, true, null, 7L, 2L, Map.of(), Map.of());
        Engine.GetResult gr = r.toGetResult();

        assertTrue(gr instanceof DocumentLookupResult.PreMaterialized);
        assertTrue(gr.exists());
        assertEquals(5L, gr.version());
        assertSame(r, ((DocumentLookupResult.PreMaterialized) gr).lookup());

        // PreMaterialized has no Lucene searcher/docId — these must fail fast, not be dereferenced.
        expectThrows(UnsupportedOperationException.class, gr::searcher);
        expectThrows(UnsupportedOperationException.class, gr::docIdAndVersion);

        gr.close(); // no-op, must not throw
    }
}
