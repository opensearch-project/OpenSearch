/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.admin.tiering;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.storage.action.tiering.IndexTieringRequest;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class IndexTieringRequestTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        IndexTieringRequest request = new IndexTieringRequest("WARM", "test-index");
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        IndexTieringRequest deserialized = new IndexTieringRequest(in);
        assertEquals("test-index", deserialized.getIndex());
        assertEquals(request, deserialized);
        assertEquals(request.hashCode(), deserialized.hashCode());
    }

    public void testValidate_Valid() {
        assertNull(new IndexTieringRequest("WARM", "test-index").validate());
    }

    public void testValidate_NullIndex() {
        assertNotNull(new IndexTieringRequest("WARM", null).validate());
    }

    public void testValidate_BlankIndex() {
        assertNotNull(new IndexTieringRequest("WARM", " ").validate());
    }

    public void testValidate_BlocklistedIndex() {
        assertNotNull(new IndexTieringRequest("WARM", ".kibana").validate());
    }

    public void testEquals() {
        IndexTieringRequest r1 = new IndexTieringRequest("WARM", "test-index");
        IndexTieringRequest r2 = new IndexTieringRequest("WARM", "test-index");
        IndexTieringRequest r3 = new IndexTieringRequest("HOT", "test-index");
        IndexTieringRequest r4 = new IndexTieringRequest("WARM", "other-index");

        assertEquals(r1, r1);
        assertEquals(r1, r2);
        assertEquals(r1.hashCode(), r2.hashCode());
        assertNotEquals(r1, r3);
        assertNotEquals(r1, r4);
        assertNotEquals(r1, null);
        assertNotEquals(r1, "string");
    }
}
