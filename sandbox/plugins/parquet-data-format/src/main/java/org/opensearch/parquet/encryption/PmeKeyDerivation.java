/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.parquet.encryption;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Objects;
/**
 * v1 PME per-file key derivation and AAD prefix construction.
 *
 * Key derivation (two-step HMAC-SHA384, mirrors Lucene storage encryption):
 *   PRK          = HMAC-SHA384(key=dataKey, data=messageId)
 *   T1           = HMAC-SHA384(key=PRK,     data=context||0x01)
 *   pmeFooterKey = first 16 bytes of T1 (TODO: Should be full 32 bytes)
 *   context      = "opensearch/parquet-pme/footer-key/v1"
 *
 * AAD prefix (binary, not JSON):
 *   u16_be(domain.len) || domain || u8(version) || u16_be(dataKeyId.len) || dataKeyId || messageId[16]
 */
public final class PmeKeyDerivation {
    /**
     * Domain-separation label for deriving PME footer keys.
     */
    private static final String FOOTER_KEY_CONTEXT = "opensearch/parquet-pme/footer-key/v1";

    /**
     * HKDF-style expand input: UTF8(FOOTER_KEY_CONTEXT) || 0x01, where 0x01 is the first block counter.
     */
    private static final byte [] FOOTER_KEY_CONTEXT_BYTES = appendHkdfCounter(FOOTER_KEY_CONTEXT.getBytes(StandardCharsets.UTF_8), (byte) 1);

    /**
     * Domain label embedded in the binary AAD prefix for encrypted Parquet files.
     */
    private static final String AAD_DOMAIN = "opensearch/parquet-pme/file/v1";

    /**
     * Version byte embedded in the binary AAD prefix format.
     */
    private static final int AAD_VERSION_BYTE = 1;

    /**
     * HMAC algorithm used for both extract and expand steps of the v1 key derivation.
     */
    private static final String HMAC_ALGORITHM = "HmacSHA384";

    /**
     * Required size of the unwrapped index data key returned by the key provider.
     */
    public static final int DATA_KEY_BYTES = 32;

    /**
     * Required size of the per-file random message_id stored in PME key metadata.
     */
    public static final int MESSAGE_ID_BYTES = 16;

    /**
     * Size of the derived Parquet footer key passed to parquet-rs.
     * TODO: For now, only write 16 bytes due to limitations in parquet-rs.
     */
    private static final int FOOTER_KEY_BYTES = 16;
    private PmeKeyDerivation() {}
    /**
     * Derives the 32-byte PME footer key from the 32-byte data key and 16-byte message_id.
     *
     * @param dataKey   32-byte unwrapped index data key
     * @param messageId 16-byte per-file random value
     * @return 32-byte derived key; caller must zero after use
     */
    public static byte[] deriveFooterKey(byte[] dataKey, byte[] messageId) {
        Objects.requireNonNull(dataKey, "dataKey must not be null");
        Objects.requireNonNull(messageId, "messageId must not be null");
        if (dataKey.length != DATA_KEY_BYTES) {
            throw new IllegalArgumentException("dataKey must be 32 bytes, got: " + dataKey.length);
        }
        if (messageId.length != MESSAGE_ID_BYTES) {
            throw new IllegalArgumentException("messageId must be 16 bytes, got: " + messageId.length);
        }
        try {
            byte[] prk = hmac(dataKey, messageId);
            byte[] t1 = hmac(prk, FOOTER_KEY_CONTEXT_BYTES);
            Arrays.fill(prk, (byte) 0);
            return Arrays.copyOf(t1, FOOTER_KEY_BYTES);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IllegalStateException("PME key derivation failed", e);
        }
    }
    /**
     * Builds the v1 binary AAD prefix for the given message_id.
     *
     * @param messageId 16-byte per-file random value
     * @return binary AAD prefix bytes
     */
    public static byte[] buildAadPrefix(byte[] messageId) {
        Objects.requireNonNull(messageId, "messageId must not be null");
        if (messageId.length != MESSAGE_ID_BYTES) {
            throw new IllegalArgumentException("messageId must be 16 bytes, got: " + messageId.length);
        }
        byte[] domainBytes = AAD_DOMAIN.getBytes(StandardCharsets.UTF_8);
        byte[] dataKeyIdBytes = PmeFileKeyMetadata.DEFAULT_DATA_KEY_ID.getBytes(StandardCharsets.UTF_8);
        int totalLen = 2 + domainBytes.length + 1 + 2 + dataKeyIdBytes.length + MESSAGE_ID_BYTES;
        byte[] aad = new byte[totalLen];
        int pos = 0;
        aad[pos++] = (byte) (domainBytes.length >>> 8);
        aad[pos++] = (byte) domainBytes.length;
        System.arraycopy(domainBytes, 0, aad, pos, domainBytes.length);
        pos += domainBytes.length;
        aad[pos++] = (byte) AAD_VERSION_BYTE;
        aad[pos++] = (byte) (dataKeyIdBytes.length >>> 8);
        aad[pos++] = (byte) dataKeyIdBytes.length;
        System.arraycopy(dataKeyIdBytes, 0, aad, pos, dataKeyIdBytes.length);
        pos += dataKeyIdBytes.length;
        System.arraycopy(messageId, 0, aad, pos, MESSAGE_ID_BYTES);
        return aad;
    }
    private static byte[] hmac(byte[] key, byte[] data) throws NoSuchAlgorithmException, InvalidKeyException {
        Mac mac = Mac.getInstance(HMAC_ALGORITHM);
        mac.init(new SecretKeySpec(key, HMAC_ALGORITHM));
        return mac.doFinal(data);
    }

    /**
     * Adds an RFC 5869 HKDF-style block counter to the given input.
     */
    private static byte[] appendHkdfCounter(byte[] input, byte counter) {
        byte[] output = new byte[input.length + 1];
        System.arraycopy(input, 0, output, 0, input.length);
        output[input.length] = counter;
        return output;
    }
}
