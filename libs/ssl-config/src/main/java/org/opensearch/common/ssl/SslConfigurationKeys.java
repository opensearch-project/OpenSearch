/*
 * SPDX-License-Identifier: Apache-2.0
 *
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

package org.opensearch.common.ssl;

import javax.net.ssl.TrustManagerFactory;

import java.security.KeyStore;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Utility class for handling the standard setting keys for use in SSL configuration.
 *
 * @see SslConfiguration
 * @see SslConfigurationLoader
 */
public class SslConfigurationKeys {
    /**
     * The SSL/TLS protocols (i.e. versions) that should be used
     */
    public static final String PROTOCOLS = "supported_protocols";

    /**
     * The SSL/TLS cipher suites that should be used
     */
    public static final String CIPHERS = "cipher_suites";

    /**
     * Whether certificate and/or hostname verification should be used
     */
    public static final String VERIFICATION_MODE = "verification_mode";

    /**
     * When operating as a server, whether to request/require client certificates
     */
    public static final String CLIENT_AUTH = "client_authentication";

    // Trust
    /**
     * A list of paths to PEM formatted certificates that should be trusted as CAs
     */
    public static final String CERTIFICATE_AUTHORITIES = "certificate_authorities";
    /**
     * The path to a KeyStore file (in a format supported by this JRE) that should be used as a trust-store
     */
    public static final String TRUSTSTORE_PATH = "truststore.path";
    /**
     * The password for the file configured in {@link #TRUSTSTORE_PATH}, as a secure setting.
     */
    public static final String TRUSTSTORE_SECURE_PASSWORD = "truststore.secure_password";
    /**
     * The {@link KeyStore#getType() keystore type} for the file configured in {@link #TRUSTSTORE_PATH}.
     */
    public static final String TRUSTSTORE_TYPE = "truststore.type";
    /**
     * The {@link TrustManagerFactory#getAlgorithm() trust management algorithm} to use when configuring trust
     * with a {@link #TRUSTSTORE_PATH truststore}.
     */
    public static final String TRUSTSTORE_ALGORITHM = "truststore.algorithm";

    // Key Management
    // -- Keystore
    /**
     * The path to a KeyStore file (in a format supported by this JRE) that should be used for key management
     */
    public static final String KEYSTORE_PATH = "keystore.path";
    /**
     * The password for the file configured in {@link #KEYSTORE_PATH}, as a secure setting.
     */
    public static final String KEYSTORE_SECURE_PASSWORD = "keystore.secure_password";
    /**
     * The password for the key within the {@link #KEYSTORE_PATH configured keystore}, as a secure setting.
     * If no key password is specified, it will default to the keystore password.
     */
    public static final String KEYSTORE_SECURE_KEY_PASSWORD = "keystore.secure_key_password";
    /**
     * The {@link KeyStore#getType() keystore type} for the file configured in {@link #KEYSTORE_PATH}.
     */
    public static final String KEYSTORE_TYPE = "keystore.type";
    /**
     * The {@link javax.net.ssl.KeyManagerFactory#getAlgorithm() key management algorithm} to use when
     * connstructing a Key manager from a {@link #KEYSTORE_PATH keystore}.
     */
    public static final String KEYSTORE_ALGORITHM = "keystore.algorithm";
    // -- PEM
    /**
     * The path to a PEM formatted file that contains the certificate to be used as part of key management
     */
    public static final String CERTIFICATE = "certificate";
    /**
     * The path to a PEM formatted file that contains the private key for the configured {@link #CERTIFICATE}.
     */
    public static final String KEY = "key";
    /**
     * The password to read the configured {@link #KEY}, as a secure setting.
     * This is required if the key file is encrypted.
     */
    public static final String KEY_SECURE_PASSPHRASE = "secure_key_passphrase";

    private SslConfigurationKeys() {
        throw new IllegalStateException("Utility class should not be instantiated");
    }

    /**
     * The list of keys that are used to load a non-secure, non-list setting
     */
    public static List<String> getStringKeys() {
        return Arrays.asList(
            VERIFICATION_MODE,
            CLIENT_AUTH,
            TRUSTSTORE_PATH,
            TRUSTSTORE_TYPE,
            TRUSTSTORE_TYPE,
            KEYSTORE_PATH,
            KEYSTORE_TYPE,
            KEYSTORE_ALGORITHM,
            CERTIFICATE,
            KEY
        );
    }

    /**
     * The list of keys that are used to load a non-secure, list setting
     */
    public static List<String> getListKeys() {
        return Arrays.asList(PROTOCOLS, CIPHERS, CERTIFICATE_AUTHORITIES);
    }

    /**
     * The list of keys that are used to load a secure setting (such as a password) that would typically be stored in the opensearch
     * keystore.
     */
    public static List<String> getSecureStringKeys() {
        return Arrays.asList(TRUSTSTORE_SECURE_PASSWORD, KEYSTORE_SECURE_PASSWORD, KEYSTORE_SECURE_KEY_PASSWORD, KEY_SECURE_PASSPHRASE);
    }

    /**
     * @return {@code true} if the provided key is a deprecated setting
     */
    public static boolean isDeprecated(String key) {
        return false;
    }
}
