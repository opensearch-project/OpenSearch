/* * SPDX-License-Identifier: Apache-2.0 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.store;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.cert.CertificateException;
import java.security.InvalidKeyException;
import java.security.UnrecoverableKeyException;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.security.auth.DestroyFailedException;
import java.util.function.Supplier;
import java.util.Objects;

/**
 * A Dummy implementation of a Key Store based on a user provided
 * {@link java.security.KeyStore} containing the master key
 *
 * @opensearch.internal
 */
public class LocalKeyStoreManager implements KeyStoreManager {
    private final KeyStore keystore;
    private final String alias;
    private final Supplier<String> keyPass;

    public static LocalKeyStoreManager load(String keyStorePath, String alias, Supplier<String> pass) throws IOException {
        try {
            Objects.requireNonNull(alias);
            Objects.requireNonNull(keyStorePath);
            Objects.requireNonNull(pass.get());
        } catch (NullPointerException npe) {
            throw new IOException("Failed to open local keystore. Check keystore parameters");
        }
        return new LocalKeyStoreManager(keyStorePath, alias, pass);
    }

    private LocalKeyStoreManager(String keyStorePath, String alias, Supplier<String> pass) throws IOException {
        try (InputStream in = Files.newInputStream(Path.of(keyStorePath), StandardOpenOption.READ)) {
            keystore = KeyStore.getInstance("PKCS12");
            keystore.load(in, pass.get().toCharArray());
            this.alias = alias;
            this.keyPass = pass;
        } catch (java.security.AccessControlException | KeyStoreException | CertificateException | NoSuchAlgorithmException e) {
            throw new IOException("Failed to open local keystore.", e);
        }
    }

    public Key generateDataKey() {
        try {
            KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
            keyGenerator.init(256);
            return keyGenerator.generateKey();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException();
        }
    }

    public byte[] wrapDataKey(Key key) {
        try {
            SecretKey master = (SecretKey) keystore.getKey(alias, keyPass.get().toCharArray());
            Cipher cipher = Cipher.getInstance("AESWRAP");
            cipher.init(Cipher.WRAP_MODE, master);
            byte[] wrappedKey = cipher.wrap(key);
            try {
                master.destroy();
            } catch (DestroyFailedException dfe) {
                master = null;
            }
            return wrappedKey;
        } catch (InvalidKeyException | IllegalBlockSizeException | NoSuchPaddingException | NoSuchAlgorithmException
            | UnrecoverableKeyException | KeyStoreException e) {
            throw new RuntimeException(e);
        }
    }

    public Key decryptDataKey(byte[] wrappedKey) {
        try {
            SecretKey master = (SecretKey) keystore.getKey(alias, keyPass.get().toCharArray());
            Cipher cipher = Cipher.getInstance("AESWRAP");
            cipher.init(Cipher.UNWRAP_MODE, master);
            Key key = (Key) cipher.unwrap(wrappedKey, "AES", Cipher.SECRET_KEY);
            try {
                master.destroy();
            } catch (DestroyFailedException dfe) {
                master = null;
            }
            return key;
        } catch (InvalidKeyException | NoSuchPaddingException | NoSuchAlgorithmException | UnrecoverableKeyException
            | KeyStoreException e) {
            throw new RuntimeException(e);
        }
    }
}
