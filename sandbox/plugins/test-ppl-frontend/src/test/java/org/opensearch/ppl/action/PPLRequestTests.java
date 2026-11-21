/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class PPLRequestTests extends OpenSearchTestCase {

    public void testDefaultConstructor() {
        PPLRequest request = new PPLRequest("source=test");
        assertEquals("source=test", request.getPplText());
        assertFalse(request.isExplain());
        assertNull(request.getTargetPartitions());
        assertNull(request.validate());
    }

    public void testExplainConstructor() {
        PPLRequest request = new PPLRequest("source=test", true);
        assertEquals("source=test", request.getPplText());
        assertTrue(request.isExplain());
        assertNull(request.getTargetPartitions());
        assertNull(request.validate());
    }

    public void testTargetPartitionsConstructor() {
        PPLRequest request = new PPLRequest("source=test", false, 8);
        assertEquals("source=test", request.getPplText());
        assertFalse(request.isExplain());
        assertEquals(Integer.valueOf(8), request.getTargetPartitions());
        assertNull(request.validate());
    }

    public void testValidationInvalidTargetPartitions() {
        PPLRequest request = new PPLRequest("source=test", false, 0);
        ActionRequestValidationException ex = request.validate();
        assertNotNull(ex);
        assertTrue(ex.getMessage().contains("targetPartitions must be >= 1"));

        PPLRequest requestNegative = new PPLRequest("source=test", false, -3);
        ActionRequestValidationException exNegative = requestNegative.validate();
        assertNotNull(exNegative);
        assertTrue(exNegative.getMessage().contains("targetPartitions must be >= 1"));
    }

    public void testValidationMissingQuery() {
        PPLRequest request = new PPLRequest((String) null);
        ActionRequestValidationException ex = request.validate();
        assertNotNull(ex);
        assertTrue(ex.getMessage().contains("pplText is missing or empty"));
    }

    public void testStreamSerializationRoundTripWithTargetPartitions() throws IOException {
        PPLRequest original = new PPLRequest("source=index | where a > 1", true, 16);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                PPLRequest decoded = new PPLRequest(in);
                assertEquals(original.getPplText(), decoded.getPplText());
                assertEquals(original.isExplain(), decoded.isExplain());
                assertEquals(original.getTargetPartitions(), decoded.getTargetPartitions());
            }
        }
    }

    public void testStreamSerializationRoundTripWithNullTargetPartitions() throws IOException {
        PPLRequest original = new PPLRequest("source=index", false, null);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                PPLRequest decoded = new PPLRequest(in);
                assertEquals(original.getPplText(), decoded.getPplText());
                assertEquals(original.isExplain(), decoded.isExplain());
                assertNull(decoded.getTargetPartitions());
            }
        }
    }

    public void testStreamSerializationWithLegacyVersion() throws IOException {
        PPLRequest original = new PPLRequest("source=index", true, 8);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(org.opensearch.Version.V_3_8_0);
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setVersion(org.opensearch.Version.V_3_8_0);
                PPLRequest decoded = new PPLRequest(in);
                assertEquals(original.getPplText(), decoded.getPplText());
                assertEquals(original.isExplain(), decoded.isExplain());
                assertNull(decoded.getTargetPartitions());
            }
        }
    }
}
