/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.arrow;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class ArrowMessageTests extends OpenSearchTestCase {

    public void testConstructorFromBytesAndGetters() {
        byte[] payload = { 1, 2, 3 };
        ArrowMessage message = new ArrowMessage(payload, 1000L);

        assertArrayEquals(payload, message.getPayload());
        assertEquals(1000L, message.getTimestamp().longValue());
    }

    public void testConstructorWithNullPayloadAndTimestamp() {
        ArrowMessage message = new ArrowMessage((byte[]) null, null);

        assertNull(message.getPayload());
        assertNull(message.getTimestamp());
    }

    public void testConstructorFromXContentBuilder() throws Exception {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        builder.field("hello", "world");
        builder.endObject();
        byte[] expectedPayload = BytesReference.toBytes(BytesReference.bytes(builder));

        ArrowMessage message = new ArrowMessage(builder, 42L);

        assertEquals(42L, message.getTimestamp().longValue());
        assertArrayEquals(expectedPayload, message.getPayload());
        assertEquals("{\"hello\":\"world\"}", new String(expectedPayload, java.nio.charset.StandardCharsets.UTF_8));
    }
}
