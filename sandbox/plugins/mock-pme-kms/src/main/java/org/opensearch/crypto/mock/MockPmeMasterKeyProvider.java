/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.crypto.mock;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.crypto.DataKeyPair;
import org.opensearch.common.crypto.MasterKeyProvider;

import java.security.SecureRandom;
import java.util.Collections;
import java.util.Map;

/**
 * TODO: Mock KMS for testing; should be also consolidated with testing approach used by Lucene encryption plugin.
 *
 * Dummy {@link MasterKeyProvider} for development and integration testing.
 *
 * <p>Generates cryptographically random 32-byte data keys via {@link SecureRandom}.
 * The "encrypted" key stored in the keyfile is identical to the raw key — {@link #decryptKey}
 * is an identity function. This satisfies the PME key-round-trip contract
 * ({@code decryptKey(encryptedKey) == rawKey}) without requiring a real KMS.
 *
 * <p><strong>NOT for production use.</strong> Any node that can read the keyfile can
 * decrypt all Parquet data without KMS involvement.
 */
public class MockPmeMasterKeyProvider implements MasterKeyProvider {

    private static final SecureRandom RANDOM = new SecureRandom();
    private static final int KEY_LENGTH_BYTES = 32;

    MockPmeMasterKeyProvider(@SuppressWarnings("unused") CryptoMetadata cryptoMetadata) {}

    /**
     * Generates a fresh random 32-byte key pair.
     *
     * <p>Both {@code rawKey} and {@code encryptedKey} in the returned pair contain the same
     * random bytes. The keyfile will store {@code encryptedKey}; {@link #decryptKey} returns
     * it unchanged. The PME derivation step in {@code PmeKeyDerivation} then produces the
     * actual per-file footer key from {@code rawKey}.
     */
    @Override
    public DataKeyPair generateDataPair() {
        byte[] key = new byte[KEY_LENGTH_BYTES];
        RANDOM.nextBytes(key);
        // Store a copy as encryptedKey so callers cannot accidentally alias both halves.
        byte[] encryptedKeyCopy = key.clone();
        return new DataKeyPair(key, encryptedKeyCopy);
    }

    /**
     * Identity decryption — returns {@code encryptedKey} unchanged.
     *
     * <p>Because {@link #generateDataPair} stores the same bytes in both fields of the pair,
     * returning the input here is equivalent to "decrypting" back to the original raw key.
     */
    @Override
    public byte[] decryptKey(byte[] encryptedKey) {
        // Return a defensive copy so callers cannot mutate the stored reference.
        return encryptedKey.clone();
    }

    /** Returns a fixed mock key ID. */
    @Override
    public String getKeyId() {
        return "mock-pme-key-id";
    }

    /** Returns an empty encryption context — no real KMS context needed. */
    @Override
    public Map<String, String> getEncryptionContext() {
        return Collections.emptyMap();
    }

    @Override
    public void close() {
        // Nothing to close — no network connections or KMS sessions.
    }
}



