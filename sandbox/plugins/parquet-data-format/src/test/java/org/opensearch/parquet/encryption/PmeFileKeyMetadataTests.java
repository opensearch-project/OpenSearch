/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.encryption;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

/**
 * Tests for {@link PmeFileKeyMetadata} — v1 JSON round-trip and rejection cases.
 */
public class PmeFileKeyMetadataTests extends OpenSearchTestCase {

    private static byte[] randomMessageId() {
        byte[] id = new byte[16];
        random().nextBytes(id);
        return id;
    }

    // ---- forNewFile ----

    public void testForNewFileRejectsNullMessageId() {
        expectThrows(NullPointerException.class, () -> PmeFileKeyMetadata.forNewFile(null));
    }

    public void testForNewFileRejectsWrongLength() {
        expectThrows(IllegalArgumentException.class, () -> PmeFileKeyMetadata.forNewFile(new byte[8]));
        expectThrows(IllegalArgumentException.class, () -> PmeFileKeyMetadata.forNewFile(new byte[32]));
    }

    public void testForNewFileSetsV1Fields() {
        byte[] messageId = randomMessageId();
        PmeFileKeyMetadata meta = PmeFileKeyMetadata.forNewFile(messageId);

        assertEquals(PmeFileKeyMetadata.V1, meta.version());
        assertEquals(PmeFileKeyMetadata.DEFAULT_DATA_KEY_ID, meta.dataKeyId());
        assertArrayEquals(messageId, meta.messageId());
    }

    public void testForNewFileDefensiveCopyOfMessageId() {
        byte[] messageId = randomMessageId();
        PmeFileKeyMetadata meta = PmeFileKeyMetadata.forNewFile(messageId);
        Arrays.fill(messageId, (byte) 0);
        // Mutating the original must not affect the stored value.
        byte[] stored = meta.messageId();
        boolean allZero = true;
        for (byte b : stored) {
            if (b != 0) {
                allZero = false;
                break;
            }
        }
        assertFalse("messageId stored inside metadata must be a defensive copy", allZero);
    }

    // ---- toJsonBytes ----

    public void testToJsonBytesProducesCompactJson() {
        byte[] messageId = new byte[16]; // all zeros for determinism
        String expectedB64 = Base64.getUrlEncoder().withoutPadding().encodeToString(messageId);
        byte[] json = PmeFileKeyMetadata.forNewFile(messageId).toJsonBytes();
        String jsonStr = new String(json, StandardCharsets.UTF_8);

        assertEquals(
            "{\"version\":1,\"data_key_id\":\"default\",\"message_id\":\"" + expectedB64 + "\"}",
            jsonStr
        );
    }

    public void testToJsonBytesIsUtf8() {
        byte[] json = PmeFileKeyMetadata.forNewFile(randomMessageId()).toJsonBytes();
        // Must be valid UTF-8 (no exception).
        String str = new String(json, StandardCharsets.UTF_8);
        assertTrue(str.startsWith("{"));
    }

    // ---- parse: happy path ----

    public void testRoundTrip() throws IOException {
        byte[] messageId = randomMessageId();
        PmeFileKeyMetadata original = PmeFileKeyMetadata.forNewFile(messageId);
        byte[] json = original.toJsonBytes();

        PmeFileKeyMetadata parsed = PmeFileKeyMetadata.parse(json);

        assertEquals(PmeFileKeyMetadata.V1, parsed.version());
        assertEquals(PmeFileKeyMetadata.DEFAULT_DATA_KEY_ID, parsed.dataKeyId());
        assertArrayEquals(messageId, parsed.messageId());
    }

    public void testParseExampleFromSpec() throws IOException {
        // Example from pme-key-management.md
        String json = "{\"version\":1,\"data_key_id\":\"default\",\"message_id\":\"VfN2M7dPjJxYpI3aLc0S6A\"}";
        PmeFileKeyMetadata meta = PmeFileKeyMetadata.parse(json.getBytes(StandardCharsets.UTF_8));

        assertEquals(1, meta.version());
        assertEquals("default", meta.dataKeyId());
        assertEquals(16, meta.messageId().length);
    }

    // ---- parse: rejection cases ----

    public void testParseRejectsNull() {
        expectThrows(NullPointerException.class, () -> PmeFileKeyMetadata.parse(null));
    }

    public void testParseRejectsEmptyBytes() {
        expectThrows(IOException.class, () -> PmeFileKeyMetadata.parse(new byte[0]));
    }

    public void testParseRejectsUnknownVersion() {
        String json = "{\"version\":99,\"data_key_id\":\"default\",\"message_id\":\"VfN2M7dPjJxYpI3aLc0S6A\"}";
        expectThrows(IOException.class, () -> PmeFileKeyMetadata.parse(json.getBytes(StandardCharsets.UTF_8)));
    }

    public void testParseRejectsWrongDataKeyId() {
        String json = "{\"version\":1,\"data_key_id\":\"custom\",\"message_id\":\"VfN2M7dPjJxYpI3aLc0S6A\"}";
        expectThrows(IOException.class, () -> PmeFileKeyMetadata.parse(json.getBytes(StandardCharsets.UTF_8)));
    }

    public void testParseRejectsMissingVersion() {
        String json = "{\"data_key_id\":\"default\",\"message_id\":\"VfN2M7dPjJxYpI3aLc0S6A\"}";
        expectThrows(IOException.class, () -> PmeFileKeyMetadata.parse(json.getBytes(StandardCharsets.UTF_8)));
    }

    public void testParseRejectsMissingDataKeyId() {
        String json = "{\"version\":1,\"message_id\":\"VfN2M7dPjJxYpI3aLc0S6A\"}";
        expectThrows(IOException.class, () -> PmeFileKeyMetadata.parse(json.getBytes(StandardCharsets.UTF_8)));
    }

    public void testParseRejectsMissingMessageId() {
        String json = "{\"version\":1,\"data_key_id\":\"default\"}";
        expectThrows(IOException.class, () -> PmeFileKeyMetadata.parse(json.getBytes(StandardCharsets.UTF_8)));
    }

    public void testParseRejectsUnknownField() {
        String json = "{\"version\":1,\"data_key_id\":\"default\",\"message_id\":\"VfN2M7dPjJxYpI3aLc0S6A\",\"extra\":\"x\"}";
        expectThrows(IOException.class, () -> PmeFileKeyMetadata.parse(json.getBytes(StandardCharsets.UTF_8)));
    }

    public void testParseRejectsMalformedBase64MessageId() {
        String json = "{\"version\":1,\"data_key_id\":\"default\",\"message_id\":\"not-valid-!!!\"}";
        expectThrows(IOException.class, () -> PmeFileKeyMetadata.parse(json.getBytes(StandardCharsets.UTF_8)));
    }

    public void testParseRejectsMessageIdDecodingToWrongLength() {
        // 8 bytes → 11 base64url chars (not 16 bytes)
        String b64 = Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[8]);
        String json = "{\"version\":1,\"data_key_id\":\"default\",\"message_id\":\"" + b64 + "\"}";
        expectThrows(IOException.class, () -> PmeFileKeyMetadata.parse(json.getBytes(StandardCharsets.UTF_8)));
    }

    public void testParseRejectsMalformedJson() {
        expectThrows(IOException.class, () -> PmeFileKeyMetadata.parse("not json".getBytes(StandardCharsets.UTF_8)));
    }

    // ---- messageId returns defensive copy ----

    public void testMessageIdReturnsCopy() throws IOException {
        byte[] messageId = randomMessageId();
        PmeFileKeyMetadata meta = PmeFileKeyMetadata.parse(PmeFileKeyMetadata.forNewFile(messageId).toJsonBytes());

        byte[] first = meta.messageId();
        Arrays.fill(first, (byte) 0xFF);
        byte[] second = meta.messageId();

        assertArrayEquals("messageId() must return a fresh copy each time", messageId, second);
    }
}

