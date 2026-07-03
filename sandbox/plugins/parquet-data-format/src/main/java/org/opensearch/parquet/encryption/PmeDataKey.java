/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.encryption;

import java.util.Arrays;
import java.util.Objects;

/**
 * Scoped 32-byte data key holder with best-effort zeroing.
 *
 * <p>Callers receive key material via {@link #bytes()}, which returns a defensive copy.
 * The internal backing array is zeroed on {@link #zero()}, which must be called before
 * the instance is discarded (e.g., on cache eviction or shard close).
 */
public final class PmeDataKey {

    private final byte[] keyBytes;

    /**
     * Creates a new key holder. The provided array is defensively copied;
     * the caller should zero their copy immediately after construction.
     *
     * <p>In production code, obtain instances via {@link PmeDataKeyCache#getOrLoad}.
     * This constructor is public to allow test-only direct construction.
     *
     * @param keyBytes raw 32-byte data key
     */
    public PmeDataKey(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes must not be null");
        if (keyBytes.length != PmeKeyDerivation.DATA_KEY_BYTES) {
            throw new IllegalArgumentException("data key must be 32 bytes, got: " + keyBytes.length);
        }
        this.keyBytes = keyBytes.clone();
    }

    /**
     * Returns a defensive copy of the 32-byte key material.
     * The caller is responsible for zeroing the returned array after use.
     */
    byte[] bytes() {
        return keyBytes.clone();
    }

    /**
     * Best-effort zeroing of the internal key material.
     * Call before discarding this instance (e.g., on eviction).
     */
    void zero() {
        Arrays.fill(keyBytes, (byte) 0);
    }
}
