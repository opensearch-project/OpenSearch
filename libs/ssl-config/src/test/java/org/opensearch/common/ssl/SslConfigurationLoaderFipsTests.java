/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.ssl;

import org.opensearch.common.settings.Settings;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import java.util.Arrays;
import java.util.Locale;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SslConfigurationLoaderFipsTests extends SslConfigurationLoaderTests {

    private static final String BCFKS_PASSWORD = "bcfks-pass";
    private static final String BCFKS = "BCFKS";

    public void testLoadTrustFromPkcs12WithoutMAC() {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
    }

    public void testLoadTrustFromPkcs12() {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
    }

    public void testLoadTrustFromJKS() {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
    }

    public void testLoadKeysFromPKCS12() {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
    }

    public void testLoadKeysFromJKS() {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
    }

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
            assertThrows("TrustEverythingConfig is not allowed in FIPS JVM", IllegalStateException.class, trustConfig::createTrustManager);
        }
    }

    public void testLoadTrustFromBCFKS() {
        final Settings.Builder builder = Settings.builder().put("test.ssl.truststore.path", "ca-all/ca.bcfks");
        if (randomBoolean()) {
            builder.put("test.ssl.truststore.password", BCFKS_PASSWORD);
        } else {
            secureSettings.setString("test.ssl.truststore.secure_password", BCFKS_PASSWORD);
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

    public void testLoadKeysFromBCFKS() {
        final Settings.Builder builder = Settings.builder().put("test.ssl.keystore.path", "cert-all/certs.bcfks");
        if (randomBoolean()) {
            builder.put("test.ssl.keystore.password", BCFKS_PASSWORD);
        } else {
            secureSettings.setString("test.ssl.keystore.secure_password", BCFKS_PASSWORD);
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
}
