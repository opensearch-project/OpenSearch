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

import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.settings.SecureString;
import org.opensearch.test.OpenSearchTestCase;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SslConfigurationLoaderTests extends OpenSearchTestCase {

    protected static final String BCFKS = "BCFKS";
    private final String STRONG_PRIVATE_SECRET = "6!6428DQXwPpi7@$ggeg/=";
    private final Path certRoot = getDataPath("/certs/ca1/ca.crt").getParent().getParent();

    private Settings settings;
    private MockSecureSettings secureSettings = new MockSecureSettings();
    private SslConfigurationLoader loader = new SslConfigurationLoader("test.ssl.") {
        @Override
        protected String getSettingAsString(String key) throws Exception {
            return settings.get(key);
        }

        @Override
        protected char[] getSecureSetting(String key) throws Exception {
            final SecureString secStr = secureSettings.getString(key);
            return secStr == null ? null : secStr.getChars();
        }

        @Override
        protected List<String> getSettingAsList(String key) throws Exception {
            return settings.getAsList(key);
        }
    };

    /**
     * A test for non-trust, non-key configurations.
     * These are straight forward and can all be tested together
     */
    public void testBasicConfigurationOptions() {
        final SslVerificationMode verificationMode = randomFrom(SslVerificationMode.values());
        final SslClientAuthenticationMode clientAuth = randomFrom(SslClientAuthenticationMode.values());
        final String[] ciphers = generateRandomStringArray(8, 12, false, false);
        final String[] protocols = generateRandomStringArray(4, 5, false, false);
        settings = Settings.builder()
            .put("test.ssl.verification_mode", verificationMode.name().toLowerCase(Locale.ROOT))
            .put("test.ssl.client_authentication", clientAuth.name().toLowerCase(Locale.ROOT))
            .putList("test.ssl.cipher_suites", ciphers)
            .putList("test.ssl.supported_protocols", protocols)
            .build();
        final SslConfiguration configuration = loader.load(certRoot);
        assertThat(configuration.getClientAuth(), is(clientAuth));
        assertThat(configuration.getVerificationMode(), is(verificationMode));
        assertThat(configuration.getCipherSuites(), equalTo(Arrays.asList(ciphers)));
        assertThat(configuration.getSupportedProtocols(), equalTo(Arrays.asList(protocols)));
        if (verificationMode == SslVerificationMode.NONE) {
            final SslTrustConfig trustConfig = configuration.getTrustConfig();
            assertThat(trustConfig, instanceOf(TrustEverythingConfig.class));

            if (inFipsJvm()) {
                assertThrows(
                    "The use of TrustEverythingConfig is not permitted in FIPS mode. This issue may be caused by the 'ssl.verification_mode=NONE' setting.",
                    IllegalStateException.class,
                    trustConfig::createTrustManager
                );
            }
        }
    }

    public void testLoadTrustFromPemCAs() {
        settings = Settings.builder().putList("test.ssl.certificate_authorities", "ca1/ca.crt", "ca2/ca.crt", "ca3/ca.crt").build();
        final SslConfiguration configuration = loader.load(certRoot);
        final SslTrustConfig trustConfig = configuration.getTrustConfig();
        assertThat(trustConfig, instanceOf(PemTrustConfig.class));
        assertThat(
            trustConfig.getDependentFiles(),
            containsInAnyOrder(getDataPath("/certs/ca1/ca.crt"), getDataPath("/certs/ca2/ca.crt"), getDataPath("/certs/ca3/ca.crt"))
        );
        assertThat(trustConfig.createTrustManager(), notNullValue());
    }

    public void testLoadTrustFromBCFKS() {
        final Settings.Builder builder = Settings.builder().put("test.ssl.truststore.path", "ca-all/ca.bcfks");
        if (randomBoolean()) {
            builder.put("test.ssl.truststore.password", "bcfks-pass");
        } else {
            secureSettings.setString("test.ssl.truststore.secure_password", "bcfks-pass");
        }
        if (randomBoolean()) {
            // If this is not set, the loader will guess from the extension
            builder.put("test.ssl.truststore.type", BCFKS);
        }
        if (randomBoolean()) {
            builder.put("test.ssl.truststore.algorithm", TrustManagerFactory.getDefaultAlgorithm());
        }
        settings = builder.build();
        final SslConfiguration configuration = loader.load(certRoot);
        final SslTrustConfig trustConfig = configuration.getTrustConfig();
        assertThat(trustConfig, instanceOf(StoreTrustConfig.class));
        assertThat(trustConfig.getDependentFiles(), containsInAnyOrder(getDataPath("/certs/ca-all/ca.bcfks")));
        assertThat(trustConfig.createTrustManager(), notNullValue());
    }

    public void testLoadTrustFromPkcs12() {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final Settings.Builder builder = Settings.builder().put("test.ssl.truststore.path", "ca-all/ca.p12");
        if (randomBoolean()) {
            builder.put("test.ssl.truststore.password", "p12-pass");
        } else {
            secureSettings.setString("test.ssl.truststore.secure_password", "p12-pass");
        }
        if (randomBoolean()) {
            // If this is not set, the loader will guess from the extension
            builder.put("test.ssl.truststore.type", "PKCS12");
        }
        if (randomBoolean()) {
            builder.put("test.ssl.truststore.algorithm", TrustManagerFactory.getDefaultAlgorithm());
        }
        settings = builder.build();
        final SslConfiguration configuration = loader.load(certRoot);
        final SslTrustConfig trustConfig = configuration.getTrustConfig();
        assertThat(trustConfig, instanceOf(StoreTrustConfig.class));
        assertThat(trustConfig.getDependentFiles(), containsInAnyOrder(getDataPath("/certs/ca-all/ca.p12")));
        assertThat(trustConfig.createTrustManager(), notNullValue());
    }

    public void testLoadTrustFromJKS() {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final Settings.Builder builder = Settings.builder().put("test.ssl.truststore.path", "ca-all/ca.jks");
        if (randomBoolean()) {
            builder.put("test.ssl.truststore.password", "jks-pass");
        } else {
            secureSettings.setString("test.ssl.truststore.secure_password", "jks-pass");
        }
        if (randomBoolean()) {
            // If this is not set, the loader will guess from the extension
            builder.put("test.ssl.truststore.type", "jks");
        }
        if (randomBoolean()) {
            builder.put("test.ssl.truststore.algorithm", TrustManagerFactory.getDefaultAlgorithm());
        }
        settings = builder.build();
        final SslConfiguration configuration = loader.load(certRoot);
        final SslTrustConfig trustConfig = configuration.getTrustConfig();
        assertThat(trustConfig, instanceOf(StoreTrustConfig.class));
        assertThat(trustConfig.getDependentFiles(), containsInAnyOrder(getDataPath("/certs/ca-all/ca.jks")));
        assertThat(trustConfig.createTrustManager(), notNullValue());
    }

    public void testLoadKeysFromPemFiles() {
        final boolean usePassword = randomBoolean();
        final boolean useLegacyPassword = usePassword && randomBoolean();
        final String certName = usePassword ? "cert2" : "cert1";
        final Settings.Builder builder = Settings.builder()
            .put("test.ssl.certificate", certName + "/" + certName + ".crt")
            .put("test.ssl.key", certName + "/" + certName + ".key");
        if (usePassword) {
            if (useLegacyPassword) {
                builder.put("test.ssl.key_passphrase", STRONG_PRIVATE_SECRET);
            } else {
                secureSettings.setString("test.ssl.secure_key_passphrase", STRONG_PRIVATE_SECRET);
            }
        }
        settings = builder.build();
        final SslConfiguration configuration = loader.load(certRoot);
        final SslKeyConfig keyConfig = configuration.getKeyConfig();
        assertThat(keyConfig, instanceOf(PemKeyConfig.class));
        assertThat(
            keyConfig.getDependentFiles(),
            containsInAnyOrder(
                getDataPath("/certs/" + certName + "/" + certName + ".crt"),
                getDataPath("/certs/" + certName + "/" + certName + ".key")
            )
        );
        assertThat(keyConfig.createKeyManager(), notNullValue());
    }

    public void testLoadKeysFromBCFKS() {
        final Settings.Builder builder = Settings.builder().put("test.ssl.keystore.path", "cert-all/certs.bcfks");
        if (randomBoolean()) {
            builder.put("test.ssl.keystore.password", "bcfks-pass");
        } else {
            secureSettings.setString("test.ssl.keystore.secure_password", "bcfks-pass");
        }
        if (randomBoolean()) {
            // If this is not set, the loader will guess from the extension
            builder.put("test.ssl.keystore.type", BCFKS);
        }
        if (randomBoolean()) {
            builder.put("test.ssl.keystore.algorithm", KeyManagerFactory.getDefaultAlgorithm());
        }
        settings = builder.build();
        final SslConfiguration configuration = loader.load(certRoot);
        final SslKeyConfig keyConfig = configuration.getKeyConfig();
        assertThat(keyConfig, instanceOf(StoreKeyConfig.class));
        assertThat(keyConfig.getDependentFiles(), containsInAnyOrder(getDataPath("/certs/cert-all/certs.bcfks")));
        assertThat(keyConfig.createKeyManager(), notNullValue());
    }

    public void testLoadKeysFromPKCS12() {
        assumeFalse("Can't use PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final Settings.Builder builder = Settings.builder().put("test.ssl.keystore.path", "cert-all/certs.p12");
        if (randomBoolean()) {
            builder.put("test.ssl.keystore.password", "p12-pass");
        } else {
            secureSettings.setString("test.ssl.keystore.secure_password", "p12-pass");
        }
        if (randomBoolean()) {
            // If this is not set, the loader will guess from the extension
            builder.put("test.ssl.keystore.type", "PKCS12");
        }
        if (randomBoolean()) {
            builder.put("test.ssl.keystore.algorithm", KeyManagerFactory.getDefaultAlgorithm());
        }
        settings = builder.build();
        final SslConfiguration configuration = loader.load(certRoot);
        final SslKeyConfig keyConfig = configuration.getKeyConfig();
        assertThat(keyConfig, instanceOf(StoreKeyConfig.class));
        assertThat(keyConfig.getDependentFiles(), containsInAnyOrder(getDataPath("/certs/cert-all/certs.p12")));
        assertThat(keyConfig.createKeyManager(), notNullValue());
    }

    public void testLoadKeysFromJKS() {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final Settings.Builder builder = Settings.builder().put("test.ssl.keystore.path", "cert-all/certs.jks");
        if (randomBoolean()) {
            builder.put("test.ssl.keystore.password", "jks-pass");
        } else {
            secureSettings.setString("test.ssl.keystore.secure_password", "jks-pass");
        }
        if (randomBoolean()) {
            builder.put("test.ssl.keystore.key_password", "key-pass");
        } else {
            secureSettings.setString("test.ssl.keystore.secure_key_password", "key-pass");
        }
        if (randomBoolean()) {
            // If this is not set, the loader will guess from the extension
            builder.put("test.ssl.keystore.type", "jks");
        }
        if (randomBoolean()) {
            builder.put("test.ssl.keystore.algorithm", KeyManagerFactory.getDefaultAlgorithm());
        }
        settings = builder.build();
        final SslConfiguration configuration = loader.load(certRoot);
        final SslKeyConfig keyConfig = configuration.getKeyConfig();
        assertThat(keyConfig, instanceOf(StoreKeyConfig.class));
        assertThat(keyConfig.getDependentFiles(), containsInAnyOrder(getDataPath("/certs/cert-all/certs.jks")));
        assertThat(keyConfig.createKeyManager(), notNullValue());
    }
}
