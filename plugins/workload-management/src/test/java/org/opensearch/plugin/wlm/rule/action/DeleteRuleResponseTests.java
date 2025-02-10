/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class DeleteRuleResponseTests extends OpenSearchTestCase {

    /**
     * Test case to verify the serialization and deserialization of DeleteRuleResponse when acknowledged is true
     */
    public void testSerializationAcknowledgedTrue() throws IOException {
        DeleteRuleResponse response = new DeleteRuleResponse(true);
        assertTrue(response.isAcknowledged());

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();

        DeleteRuleResponse otherResponse = new DeleteRuleResponse(streamInput);
        assertEquals(response.isAcknowledged(), otherResponse.isAcknowledged());
    }

    /**
     * Test case to verify the serialization and deserialization of DeleteRuleResponse when acknowledged is false
     */
    public void testSerializationAcknowledgedFalse() throws IOException {
        DeleteRuleResponse response = new DeleteRuleResponse(false);
        assertFalse(response.isAcknowledged());

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();

        DeleteRuleResponse otherResponse = new DeleteRuleResponse(streamInput);
        assertEquals(response.isAcknowledged(), otherResponse.isAcknowledged());
    }

    /**
     * Test case to verify the toXContent output of DeleteRuleResponse when acknowledged is true
     */
    public void testToXContentAcknowledgedTrue() throws IOException {
        DeleteRuleResponse response = new DeleteRuleResponse(true);
        String actual = response.toXContentString();
        String expected = "{\n" +
            "  \"acknowledged\" : true\n" +
            "}";
        assertEquals(expected, actual);
    }

    /**
     * Test case to verify the toXContent output of DeleteRuleResponse when acknowledged is false
     */
    public void testToXContentAcknowledgedFalse() throws IOException {
        DeleteRuleResponse response = new DeleteRuleResponse(false);
        String actual = response.toXContentString();
        String expected = "{\n" +
            "  \"acknowledged\" : false\n" +
            "}";
        assertEquals(expected, actual);
    }
}


