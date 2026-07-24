/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.opensearch.index.IngestionPayloadDecodingException;
import org.opensearch.index.Message;
import org.opensearch.index.engine.FakeIngestionSource;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

public class XContentIngestionPayloadDecoderTests extends OpenSearchTestCase {

    public void testValidateAcceptsEmptySettings() {
        XContentIngestionPayloadDecoder.Factory.INSTANCE.validate(Collections.emptyMap());
    }

    public void testValidateRejectsNonEmptySettings() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> XContentIngestionPayloadDecoder.Factory.INSTANCE.validate(Map.of("some_key", "some_value"))
        );
        assertTrue(e.getMessage().contains("some_key"));
    }

    public void testCreateReturnsNewInstanceEachTime() {
        XContentIngestionPayloadDecoder.Factory factory = XContentIngestionPayloadDecoder.Factory.INSTANCE;
        assertNotSame(factory.create(null, 0, Collections.emptyMap()), factory.create(null, 0, Collections.emptyMap()));
    }

    public void testDecodeValidPayload() {
        XContentIngestionPayloadDecoder decoder = new XContentIngestionPayloadDecoder();
        byte[] payload = "{\"name\":\"alice\",\"age\":30}".getBytes(StandardCharsets.UTF_8);
        Message<?> message = new FakeIngestionSource.FakeIngestionMessage(payload);

        Map<String, Object> result = decoder.decode(message);

        assertEquals("alice", result.get("name"));
        assertEquals(30, result.get("age"));
    }

    public void testDecodeInvalidPayloadThrows() {
        XContentIngestionPayloadDecoder decoder = new XContentIngestionPayloadDecoder();
        byte[] payload = "not a json".getBytes(StandardCharsets.UTF_8);
        Message<?> message = new FakeIngestionSource.FakeIngestionMessage(payload);

        expectThrows(IngestionPayloadDecodingException.class, () -> decoder.decode(message));
    }
}
