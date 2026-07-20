/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.crypto;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Shard-level encryption context carrying key provider details, data key pairs,
 * and encryption metadata across format and engine boundaries (Layer 0 contract).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ShardCryptoContext {

    private final String keyProviderName;
    private final String keyProviderType;
    private final String keyArn;
    private final DataKeyPair dataKeyPair;
    private final Map<String, String> encryptionContext;

    public ShardCryptoContext(
        String keyProviderName,
        String keyProviderType,
        String keyArn,
        DataKeyPair dataKeyPair,
        Map<String, String> encryptionContext
    ) {
        this.keyProviderName = Objects.requireNonNull(keyProviderName, "keyProviderName must not be null");
        this.keyProviderType = keyProviderType != null ? keyProviderType : "default";
        this.keyArn = keyArn;
        this.dataKeyPair = dataKeyPair;
        this.encryptionContext = encryptionContext != null ? Map.copyOf(encryptionContext) : Collections.emptyMap();
    }

    /**
     * Returns the configured key provider name.
     */
    public String getKeyProviderName() {
        return keyProviderName;
    }

    /**
     * Returns the key provider type (e.g., "aws-kms", "keystore").
     */
    public String getKeyProviderType() {
        return keyProviderType;
    }

    /**
     * Returns the KMS Key ARN if configured.
     */
    public Optional<String> getKeyArn() {
        return Optional.ofNullable(keyArn);
    }

    /**
     * Returns the data key pair associated with this shard context.
     */
    public Optional<DataKeyPair> getDataKeyPair() {
        return Optional.ofNullable(dataKeyPair);
    }

    /**
     * Returns key-value encryption context pairs.
     */
    public Map<String, String> getEncryptionContext() {
        return encryptionContext;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardCryptoContext that = (ShardCryptoContext) o;
        return Objects.equals(keyProviderName, that.keyProviderName)
            && Objects.equals(keyProviderType, that.keyProviderType)
            && Objects.equals(keyArn, that.keyArn)
            && Objects.equals(dataKeyPair, that.dataKeyPair)
            && Objects.equals(encryptionContext, that.encryptionContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyProviderName, keyProviderType, keyArn, dataKeyPair, encryptionContext);
    }

    @Override
    public String toString() {
        return "ShardCryptoContext{"
            + "keyProviderName='"
            + keyProviderName
            + '\''
            + ", keyProviderType='"
            + keyProviderType
            + '\''
            + ", keyArn='"
            + keyArn
            + '\''
            + '}';
    }
}
