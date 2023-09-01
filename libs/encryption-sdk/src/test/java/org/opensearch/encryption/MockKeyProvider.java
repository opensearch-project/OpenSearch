/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.encryption;

import javax.crypto.spec.SecretKeySpec;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;

import com.amazonaws.encryptionsdk.CryptoAlgorithm;
import com.amazonaws.encryptionsdk.DataKey;
import com.amazonaws.encryptionsdk.MasterKey;
import com.amazonaws.encryptionsdk.exception.AwsCryptoException;
import com.amazonaws.encryptionsdk.exception.UnsupportedProviderException;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class MockKeyProvider extends MasterKey {

    private static final String keyId = "test-key-id";

    public static byte[] loadFile(String file) {
        byte[] content;
        try {
            InputStream in = MockKeyProvider.class.getResourceAsStream(file);
            StringBuilder stringBuilder = new StringBuilder();
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            for (String line; (line = bufferedReader.readLine()) != null;) {
                stringBuilder.append(line);
            }
            content = stringBuilder.toString().getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalArgumentException("File " + file + " cannot be read correctly.");
        }
        String text = new String(content, StandardCharsets.UTF_8);

        String[] byteValues = text.substring(1, text.length() - 1).split(",");
        byte[] bytes = new byte[byteValues.length];

        for (int i = 0, len = bytes.length; i < len; i++) {
            bytes[i] = Byte.parseByte(byteValues[i].trim());
        }

        return bytes;
    }

    private static final byte[] rawKey = loadFile("/raw_key");
    private static final byte[] encryptedKey = loadFile("/encrypted_key");

    @Override
    public String getProviderId() {
        return "sample-provider-id";
    }

    @Override
    public String getKeyId() {
        return "Sample-key-id";
    }

    @Override
    public DataKey encryptDataKey(CryptoAlgorithm algorithm, Map encryptionContext, DataKey dataKey) {
        throw new UnsupportedOperationException("Multiple data-key encryption is not supported.");
    }

    @Override
    public DataKey generateDataKey(CryptoAlgorithm algorithm, Map encryptionContext) {
        final SecretKeySpec key = new SecretKeySpec(rawKey, algorithm.getDataKeyAlgo());
        return new DataKey(key, encryptedKey, getKeyId().getBytes(StandardCharsets.UTF_8), this);
    }

    @Override
    public DataKey decryptDataKey(CryptoAlgorithm algorithm, Collection collection, Map encryptionContext)
        throws UnsupportedProviderException, AwsCryptoException {
        return new DataKey<>(
            new SecretKeySpec(rawKey, algorithm.getDataKeyAlgo()),
            encryptedKey,
            keyId.getBytes(StandardCharsets.UTF_8),
            this
        );
    }

    static class DataKeyPair {
        private final byte[] rawKey;
        private final byte[] encryptedKey;

        public DataKeyPair(byte[] rawKey, byte[] encryptedKey) {
            this.rawKey = rawKey;
            this.encryptedKey = encryptedKey;
        }

        public byte[] getRawKey() {
            return this.rawKey;
        }

        public byte[] getEncryptedKey() {
            return this.encryptedKey;
        }
    }

}
