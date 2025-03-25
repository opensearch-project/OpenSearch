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
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.mockito.Mockito.mock;

public class DeleteRuleResponseTests extends OpenSearchTestCase {

    /**
     * Test serialization and deserialization of DeleteRuleResponse (acknowledged = true)
     */
    public void testSerializationAcknowledgedTrue() throws IOException {
        DeleteRuleResponse response = new DeleteRuleResponse(true, RestStatus.OK);
        assertTrue(response.isAcknowledged());
        assertEquals(RestStatus.OK, response.getStatus());

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        DeleteRuleResponse deserialized = new DeleteRuleResponse(in);

        assertTrue(deserialized.isAcknowledged());
        assertEquals(RestStatus.OK, deserialized.getStatus());
    }

    /**
     * Test serialization and deserialization of DeleteRuleResponse (acknowledged = false)
     */
    public void testSerializationAcknowledgedFalse() throws IOException {
        DeleteRuleResponse response = new DeleteRuleResponse(false, RestStatus.NOT_FOUND);
        assertFalse(response.isAcknowledged());
        assertEquals(RestStatus.NOT_FOUND, response.getStatus());

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        DeleteRuleResponse deserialized = new DeleteRuleResponse(in);

        assertFalse(deserialized.isAcknowledged());
        assertEquals(RestStatus.NOT_FOUND, deserialized.getStatus());
    }

    /**
     * Test toXContent output of DeleteRuleResponse
     */
    public void testToXContent() throws IOException {
        DeleteRuleResponse response = new DeleteRuleResponse(true, RestStatus.OK);
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        String actual = response.toXContent(builder, mock(ToXContent.Params.class)).toString();

        String expected = "{\n  \"acknowledged\" : true\n}";
        assertEquals(expected, actual);
    }
}

