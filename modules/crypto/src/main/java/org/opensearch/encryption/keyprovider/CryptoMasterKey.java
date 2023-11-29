/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.encryption.keyprovider;

import org.opensearch.common.crypto.DataKeyPair;
import org.opensearch.common.crypto.MasterKeyProvider;

import javax.crypto.spec.SecretKeySpec;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;

import com.amazonaws.encryptionsdk.CryptoAlgorithm;
import com.amazonaws.encryptionsdk.DataKey;
import com.amazonaws.encryptionsdk.EncryptedDataKey;
import com.amazonaws.encryptionsdk.MasterKey;
import com.amazonaws.encryptionsdk.exception.AwsCryptoException;

public class CryptoMasterKey extends MasterKey<CryptoMasterKey> implements Closeable {
    private final MasterKeyProvider keyProvider;
    private final String keyProviderName;
    private final String cryptoAlgorithm;

    public CryptoMasterKey(MasterKeyProvider keyProvider, String keyProviderName, String cryptoAlgorithm) {
        this.keyProvider = keyProvider;
        this.keyProviderName = keyProviderName;
        this.cryptoAlgorithm = cryptoAlgorithm;
    }

    @Override
    public String getProviderId() {
        return keyProviderName;
    }

    @Override
    public String getKeyId() {
        return keyProvider.getKeyId();
    }

    @Override
    public DataKey<CryptoMasterKey> generateDataKey(CryptoAlgorithm algorithm, Map<String, String> encryptionContext) {
        DataKeyPair dataKeyPairResponse = keyProvider.generateDataPair();
        final SecretKeySpec key = new SecretKeySpec(dataKeyPairResponse.getRawKey(), cryptoAlgorithm);
        return new DataKey<>(key, dataKeyPairResponse.getEncryptedKey(), getKeyId().getBytes(StandardCharsets.UTF_8), this);
    }

    @Override
    public DataKey<CryptoMasterKey> encryptDataKey(CryptoAlgorithm algorithm, Map<String, String> encryptionContext, DataKey<?> dataKey) {
        throw new UnsupportedOperationException("Multiple data-key encryption is not supported.");
    }

    @Override
    public DataKey<CryptoMasterKey> decryptDataKey(
        CryptoAlgorithm algorithm,
        Collection<? extends EncryptedDataKey> encryptedDataKeys,
        Map<String, String> encryptionContext
    ) throws AwsCryptoException {
        if (encryptedDataKeys == null || encryptedDataKeys.isEmpty()) {
            throw new IllegalArgumentException("No encrypted data key passed for decryption.");
        }
        EncryptedDataKey encryptedDataKey = encryptedDataKeys.iterator().next();
        final String keyId = new String(encryptedDataKey.getProviderInformation(), StandardCharsets.UTF_8);
        if (!this.getKeyId().equals(keyId)) {
            throw new IllegalArgumentException("Invalid provider info present in encrypted key.");
        }

        byte[] encryptedKey = encryptedDataKey.getEncryptedDataKey();
        byte[] rawKey = keyProvider.decryptKey(encryptedKey);
        return new DataKey<>(new SecretKeySpec(rawKey, cryptoAlgorithm), encryptedKey, keyId.getBytes(StandardCharsets.UTF_8), this);
    }

    @Override
    public void close() throws IOException {
        keyProvider.close();
    }
}
