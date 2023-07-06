/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.crypto;

import org.opensearch.common.settings.Settings;
import org.opensearch.cryptospi.CryptoKeyProviderExtension;
import org.opensearch.cryptospi.DataKeyPair;
import org.opensearch.cryptospi.MasterKeyProvider;
import org.opensearch.plugins.Plugin;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class MockExtensionPlugin extends Plugin implements CryptoKeyProviderExtension {

    public static byte[] loadFile(String file) {
        byte[] content;
        try {
            InputStream in = MockExtensionPlugin.class.getResourceAsStream(file);
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
    public MasterKeyProvider createKeyProvider(Settings settings) {
        return new MasterKeyProvider() {
            @Override
            public DataKeyPair generateDataPair() {
                return new DataKeyPair(rawKey, encryptedKey);
            }

            @Override
            public byte[] decryptKey(byte[] encryptedKey1) {
                return rawKey;
            }

            @Override
            public String getKeyId() {
                return "sample-key";
            }

            @Override
            public void close() {

            }
        };
    }

    @Override
    public String type() {
        return "sample-key-provider-extension";
    }

}
