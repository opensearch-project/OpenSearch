/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.encryption;

import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Tests for {@link PmeKeyDerivation} — derivation correctness and AAD prefix structure.
 */
public class PmeKeyDerivationTests extends OpenSearchTestCase {

    private static final int DATA_KEY_BYTES = 32;
    private static final int MESSAGE_ID_BYTES = 16;
    private static final int FOOTER_KEY_BYTES = 16; // TODO to be changed when 32 bit keys get available in parquet-rs

    private static byte[] dataKey(byte fill) {
        byte[] k = new byte[DATA_KEY_BYTES];
        Arrays.fill(k, fill);
        return k;
    }

    private static byte[] messageId(byte fill) {
        byte[] id = new byte[MESSAGE_ID_BYTES];
        Arrays.fill(id, fill);
        return id;
    }

    // ---- deriveFooterKey: input validation ----

    public void testDeriveFooterKeyRejectsNullDataKey() {
        expectThrows(NullPointerException.class, () -> PmeKeyDerivation.deriveFooterKey(null, messageId((byte) 1)));
    }

    public void testDeriveFooterKeyRejectsNullMessageId() {
        expectThrows(NullPointerException.class, () -> PmeKeyDerivation.deriveFooterKey(dataKey((byte) 1), null));
    }

    public void testDeriveFooterKeyRejectsShortDataKey() {
        expectThrows(IllegalArgumentException.class, () -> PmeKeyDerivation.deriveFooterKey(new byte[16], messageId((byte) 1)));
    }

    public void testDeriveFooterKeyRejectsLongDataKey() {
        expectThrows(IllegalArgumentException.class, () -> PmeKeyDerivation.deriveFooterKey(new byte[64], messageId((byte) 1)));
    }

    public void testDeriveFooterKeyRejectsShortMessageId() {
        expectThrows(IllegalArgumentException.class, () -> PmeKeyDerivation.deriveFooterKey(dataKey((byte) 1), new byte[8]));
    }

    public void testDeriveFooterKeyRejectsLongMessageId() {
        expectThrows(IllegalArgumentException.class, () -> PmeKeyDerivation.deriveFooterKey(dataKey((byte) 1), new byte[32]));
    }

    // ---- deriveFooterKey: output properties ----

    public void testDeriveFooterKeyIsDeterministic() {
        byte[] dk = dataKey((byte) 0x42);
        byte[] mid = messageId((byte) 0x13);
        byte[] k1 = PmeKeyDerivation.deriveFooterKey(dk, mid);
        byte[] k2 = PmeKeyDerivation.deriveFooterKey(dk, mid);
        assertArrayEquals("derivation must be deterministic", k1, k2);
    }

    public void testDeriveFooterKeyDiffersForDifferentMessageIds() {
        byte[] dk = dataKey((byte) 0x55);
        byte[] k1 = PmeKeyDerivation.deriveFooterKey(dk, messageId((byte) 0x01));
        byte[] k2 = PmeKeyDerivation.deriveFooterKey(dk, messageId((byte) 0x02));
        assertFalse("different message_ids must produce different keys", Arrays.equals(k1, k2));
    }

    public void testDeriveFooterKeyDiffersForDifferentDataKeys() {
        byte[] mid = messageId((byte) 0x33);
        byte[] k1 = PmeKeyDerivation.deriveFooterKey(dataKey((byte) 0x11), mid);
        byte[] k2 = PmeKeyDerivation.deriveFooterKey(dataKey((byte) 0x22), mid);
        assertFalse("different data keys must produce different footer keys", Arrays.equals(k1, k2));
    }

    public void testDeriveFooterKeyKnownVector() {
        // Zero data key + zero message_id: derive and check length/non-zero (no hardcoded HMAC bytes,
        // but at least verify derivation runs end-to-end and produces a non-trivially-zero result).
        byte[] dk = new byte[DATA_KEY_BYTES];
        byte[] mid = new byte[MESSAGE_ID_BYTES];
        byte[] key = PmeKeyDerivation.deriveFooterKey(dk, mid);
        assertEquals(FOOTER_KEY_BYTES, key.length);
        // HMAC-SHA384 of all-zero input with all-zero key is not all-zeros.
        boolean allZero = true;
        for (byte b : key) {
            if (b != 0) { allZero = false; break; }
        }
        assertFalse("derived key must not be all-zero for all-zero inputs", allZero);
    }

    // ---- buildAadPrefix: input validation ----

    public void testBuildAadPrefixRejectsNullMessageId() {
        expectThrows(NullPointerException.class, () -> PmeKeyDerivation.buildAadPrefix(null));
    }

    public void testBuildAadPrefixRejectsWrongLength() {
        expectThrows(IllegalArgumentException.class, () -> PmeKeyDerivation.buildAadPrefix(new byte[8]));
        expectThrows(IllegalArgumentException.class, () -> PmeKeyDerivation.buildAadPrefix(new byte[32]));
    }

    // ---- buildAadPrefix: structure ----

    public void testBuildAadPrefixStructure() {
        byte[] mid = messageId((byte) 0x77);
        byte[] aad = PmeKeyDerivation.buildAadPrefix(mid);

        // domain = "opensearch/parquet-pme/file/v1" (30 bytes)
        byte[] domain = "opensearch/parquet-pme/file/v1".getBytes(StandardCharsets.UTF_8);
        assertEquals(30, domain.length);

        // dataKeyId = "default" (7 bytes)
        byte[] dataKeyId = "default".getBytes(StandardCharsets.UTF_8);

        int expectedLen = 2 + domain.length + 1 + 2 + dataKeyId.length + MESSAGE_ID_BYTES;
        assertEquals("AAD prefix length must match spec", expectedLen, aad.length);

        // u16_be(domain.length)
        assertEquals((byte) 0, aad[0]);
        assertEquals((byte) 30, aad[1]);

        // domain bytes
        for (int i = 0; i < domain.length; i++) {
            assertEquals("domain byte " + i, domain[i], aad[2 + i]);
        }

        int pos = 2 + domain.length;
        // u8(version) = 1
        assertEquals((byte) 1, aad[pos++]);
        // u16_be(dataKeyId.length) = 7
        assertEquals((byte) 0, aad[pos++]);
        assertEquals((byte) 7, aad[pos++]);
        // dataKeyId bytes
        for (int i = 0; i < dataKeyId.length; i++) {
            assertEquals("dataKeyId byte " + i, dataKeyId[i], aad[pos + i]);
        }
        pos += dataKeyId.length;
        // messageId[16]
        for (int i = 0; i < MESSAGE_ID_BYTES; i++) {
            assertEquals("messageId byte " + i, mid[i], aad[pos + i]);
        }
    }

    public void testBuildAadPrefixIsDeterministic() {
        byte[] mid = messageId((byte) 0x11);
        assertArrayEquals(PmeKeyDerivation.buildAadPrefix(mid), PmeKeyDerivation.buildAadPrefix(mid));
    }

    public void testBuildAadPrefixDiffersForDifferentMessageIds() {
        byte[] aad1 = PmeKeyDerivation.buildAadPrefix(messageId((byte) 0x01));
        byte[] aad2 = PmeKeyDerivation.buildAadPrefix(messageId((byte) 0x02));
        assertFalse("different message_ids must produce different AAD prefixes", Arrays.equals(aad1, aad2));
    }
}

