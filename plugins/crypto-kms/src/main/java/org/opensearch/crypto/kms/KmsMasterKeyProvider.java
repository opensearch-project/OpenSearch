/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.crypto.kms;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.model.DataKeySpec;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.crypto.DataKeyPair;
import org.opensearch.common.crypto.MasterKeyProvider;

import java.util.Map;
import java.util.function.Supplier;

public class KmsMasterKeyProvider implements MasterKeyProvider {
    private final Map<String, String> encryptionContext;
    private final String keyArn;
    private final Supplier<AmazonKmsClientReference> clientReferenceSupplier;

    private static final Logger logger = LogManager.getLogger(KmsMasterKeyProvider.class);

    public KmsMasterKeyProvider(
        Map<String, String> encryptionContext,
        String keyArn,
        Supplier<AmazonKmsClientReference> clientReferenceSupplier
    ) {
        this.encryptionContext = encryptionContext;
        this.keyArn = keyArn;
        this.clientReferenceSupplier = clientReferenceSupplier;
    }

    @Override
    public DataKeyPair generateDataPair() {
        logger.info("Generating new data key pair");
        try (AmazonKmsClientReference clientReference = clientReferenceSupplier.get()) {
            GenerateDataKeyRequest request = GenerateDataKeyRequest.builder()
                .encryptionContext(encryptionContext)
                // Currently only 32 byte data key is supported. To add support for other key sizes add key providers
                // in org.opensearch.encryption.CryptoManagerFactory.createCryptoProvider.
                .keySpec(DataKeySpec.AES_256)
                .keyId(keyArn)
                .build();
            GenerateDataKeyResponse dataKeyPair = SocketAccess.doPrivileged(() -> clientReference.get().generateDataKey(request));
            return new DataKeyPair(dataKeyPair.plaintext().asByteArray(), dataKeyPair.ciphertextBlob().asByteArray());
        }
    }

    @Override
    public byte[] decryptKey(byte[] encryptedKey) {
        try (AmazonKmsClientReference clientReference = clientReferenceSupplier.get()) {
            DecryptRequest decryptRequest = DecryptRequest.builder()
                .ciphertextBlob(SdkBytes.fromByteArray(encryptedKey))
                .encryptionContext(encryptionContext)
                .build();
            DecryptResponse decryptResponse = SocketAccess.doPrivileged(() -> clientReference.get().decrypt(decryptRequest));
            return decryptResponse.plaintext().asByteArray();
        }
    }

    @Override
    public String getKeyId() {
        return keyArn;
    }

    @Override
    public Map<String, String> getEncryptionContext() {
        return encryptionContext;
    }

    @Override
    public void close() {}
}
