/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.encryption;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Objects;

/**
 * Per-file PME (Parquet Modular Encryption) inputs for a single Parquet file.
 *
 * <p>Bundles the derived 32-byte AES-GCM footer key, the key-metadata JSON bytes
 * (stored verbatim in {@code FileCryptoMetaData.key_metadata}), and the binary
 * AAD prefix (used for all GCM ciphertexts in the file).
 *
 * <p>Instances are single-use. Call {@link #zero()} after the native writer has
 * consumed the key material to minimise the key's time in heap memory.
 *
 * <p>Thread-safety: not thread-safe; do not share between threads.
 */
public final class PmeFileEncryptionInputs {

    private final byte[] footerKey;       // 32 bytes, zeroed after native use
    private final byte[] keyMetadataJson; // UTF-8 JSON stored in Parquet footer
    private final byte[] aadPrefix;       // binary AAD prefix for all ciphertexts

    private PmeFileEncryptionInputs(byte[] footerKey, byte[] keyMetadataJson, byte[] aadPrefix) {
        this.footerKey = footerKey;
        this.keyMetadataJson = keyMetadataJson;
        this.aadPrefix = aadPrefix;
    }

    /**
     * Creates per-file encryption inputs for a new Parquet file.
     *
     * <p>Generates a random 16-byte {@code message_id}, derives the 32-byte footer key
     * via {@link PmeKeyDerivation#deriveFooterKey}, builds the v1 key-metadata JSON via
     * {@link PmeFileKeyMetadata}, and constructs the binary AAD prefix via
     * {@link PmeKeyDerivation#buildAadPrefix}.
     *
     * @param dataKey the 32-byte index data key; its internal bytes are NOT zeroed here
     * @return per-file encryption inputs ready to be passed to the native writer
     */
    public static PmeFileEncryptionInputs create(PmeDataKey dataKey) {
        Objects.requireNonNull(dataKey, "dataKey must not be null");
        byte[] messageId = new byte[PmeKeyDerivation.MESSAGE_ID_BYTES];
        new SecureRandom().nextBytes(messageId);

        byte[] dataKeyBytes = dataKey.bytes();
        byte[] footerKey;
        try {
            footerKey = PmeKeyDerivation.deriveFooterKey(dataKeyBytes, messageId);
        } finally {
            Arrays.fill(dataKeyBytes, (byte) 0);
        }

        byte[] keyMetadataJson = PmeFileKeyMetadata.forNewFile(messageId).toJsonBytes();
        byte[] aadPrefix = PmeKeyDerivation.buildAadPrefix(messageId);
        Arrays.fill(messageId, (byte) 0);

        return new PmeFileEncryptionInputs(footerKey, keyMetadataJson, aadPrefix);
    }

    /**
     * Creates inputs for <em>decryption only</em> from already-known key material.
     *
     * <p>Used when reading back an encrypted file: the footer key and AAD prefix
     * are reconstructed from the data key and message_id parsed from the file footer.
     * The {@code keyMetadataJson} is left empty since it is not needed for reads.
     *
     * @param footerKey 32-byte derived footer key; defensive copy is taken
     * @param aadPrefix binary AAD prefix; defensive copy is taken
     * @return decryption-only inputs
     */
    public static PmeFileEncryptionInputs forDecryption(byte[] footerKey, byte[] aadPrefix) {
        Objects.requireNonNull(footerKey, "footerKey must not be null");
        Objects.requireNonNull(aadPrefix, "aadPrefix must not be null");
        return new PmeFileEncryptionInputs(footerKey.clone(), new byte[0], aadPrefix.clone());
    }

    /**
     * Returns a defensive copy of the derived footer key.
     * The caller should zero the returned array after use.
     */
    public byte[] footerKey() {
        return footerKey.clone();
    }

    /**
     * Returns a defensive copy of the key-metadata JSON bytes.
     */
    public byte[] keyMetadataJson() {
        return keyMetadataJson.clone();
    }

    /**
     * Returns a defensive copy of the binary AAD prefix.
     * The caller should zero the returned array after use.
     */
    public byte[] aadPrefix() {
        return aadPrefix.clone();
    }

    /**
     * Best-effort zeroing of the footer key and AAD prefix held by this instance.
     * Call this after the native writer has consumed the key material.
     */
    public void zero() {
        Arrays.fill(footerKey, (byte) 0);
        Arrays.fill(aadPrefix, (byte) 0);
    }
}

