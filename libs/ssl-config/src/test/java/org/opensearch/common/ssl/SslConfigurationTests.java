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

import org.opensearch.test.EqualsHashCodeTestUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.Matchers;

import javax.net.ssl.SSLContext;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.mockito.Mockito;

import static org.opensearch.common.ssl.SslConfigurationLoader.DEFAULT_CIPHERS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class SslConfigurationTests extends OpenSearchTestCase {

    static final String[] VALID_PROTOCOLS = { "TLSv1.2", "TLSv1.1", "TLSv1", "SSLv3", "SSLv2Hello", "SSLv2" };

    public void testBasicConstruction() {
        final SslTrustConfig trustConfig = Mockito.mock(SslTrustConfig.class);
        Mockito.when(trustConfig.toString()).thenReturn("TEST-TRUST");
        final SslKeyConfig keyConfig = Mockito.mock(SslKeyConfig.class);
        Mockito.when(keyConfig.toString()).thenReturn("TEST-KEY");
        final SslVerificationMode verificationMode = randomFrom(SslVerificationMode.values());
        final SslClientAuthenticationMode clientAuth = randomFrom(SslClientAuthenticationMode.values());
        final List<String> ciphers = randomSubsetOf(randomIntBetween(1, DEFAULT_CIPHERS.size()), DEFAULT_CIPHERS);
        final List<String> protocols = randomSubsetOf(randomIntBetween(1, 4), VALID_PROTOCOLS);
        final SslConfiguration configuration = new SslConfiguration(
            trustConfig,
            keyConfig,
            verificationMode,
            clientAuth,
            ciphers,
            protocols
        );

        assertThat(configuration.getTrustConfig(), is(trustConfig));
        assertThat(configuration.getKeyConfig(), is(keyConfig));
        assertThat(configuration.getVerificationMode(), is(verificationMode));
        assertThat(configuration.getClientAuth(), is(clientAuth));
        assertThat(configuration.getCipherSuites(), is(ciphers));
        assertThat(configuration.getSupportedProtocols(), is(protocols));

        assertThat(configuration.toString(), containsString("TEST-TRUST"));
        assertThat(configuration.toString(), containsString("TEST-KEY"));
        assertThat(configuration.toString(), containsString(verificationMode.toString()));
        assertThat(configuration.toString(), containsString(clientAuth.toString()));
        assertThat(configuration.toString(), containsString(randomFrom(ciphers)));
        assertThat(configuration.toString(), containsString(randomFrom(protocols)));
    }

    public void testEqualsAndHashCode() {
        final SslTrustConfig trustConfig = Mockito.mock(SslTrustConfig.class);
        final SslKeyConfig keyConfig = Mockito.mock(SslKeyConfig.class);
        final SslVerificationMode verificationMode = randomFrom(SslVerificationMode.values());
        final SslClientAuthenticationMode clientAuth = randomFrom(SslClientAuthenticationMode.values());
        final List<String> ciphers = randomSubsetOf(randomIntBetween(1, DEFAULT_CIPHERS.size() - 1), DEFAULT_CIPHERS);
        final List<String> protocols = randomSubsetOf(randomIntBetween(1, VALID_PROTOCOLS.length - 1), VALID_PROTOCOLS);
        final SslConfiguration configuration = new SslConfiguration(
            trustConfig,
            keyConfig,
            verificationMode,
            clientAuth,
            ciphers,
            protocols
        );

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            configuration,
            orig -> new SslConfiguration(
                orig.getTrustConfig(),
                orig.getKeyConfig(),
                orig.getVerificationMode(),
                orig.getClientAuth(),
                orig.getCipherSuites(),
                orig.getSupportedProtocols()
            ),
            orig -> {
                switch (randomIntBetween(1, 4)) {
                    case 1:
                        return new SslConfiguration(
                            orig.getTrustConfig(),
                            orig.getKeyConfig(),
                            randomValueOtherThan(orig.getVerificationMode(), () -> randomFrom(SslVerificationMode.values())),
                            orig.getClientAuth(),
                            orig.getCipherSuites(),
                            orig.getSupportedProtocols()
                        );
                    case 2:
                        return new SslConfiguration(
                            orig.getTrustConfig(),
                            orig.getKeyConfig(),
                            orig.getVerificationMode(),
                            randomValueOtherThan(orig.getClientAuth(), () -> randomFrom(SslClientAuthenticationMode.values())),
                            orig.getCipherSuites(),
                            orig.getSupportedProtocols()
                        );
                    case 3:
                        return new SslConfiguration(
                            orig.getTrustConfig(),
                            orig.getKeyConfig(),
                            orig.getVerificationMode(),
                            orig.getClientAuth(),
                            DEFAULT_CIPHERS,
                            orig.getSupportedProtocols()
                        );
                    case 4:
                    default:
                        return new SslConfiguration(
                            orig.getTrustConfig(),
                            orig.getKeyConfig(),
                            orig.getVerificationMode(),
                            orig.getClientAuth(),
                            orig.getCipherSuites(),
                            Arrays.asList(VALID_PROTOCOLS)
                        );
                }
            }
        );
    }

    public void testDependentFiles() {
        final SslTrustConfig trustConfig = Mockito.mock(SslTrustConfig.class);
        final SslKeyConfig keyConfig = Mockito.mock(SslKeyConfig.class);
        final SslConfiguration configuration = new SslConfiguration(
            trustConfig,
            keyConfig,
            randomFrom(SslVerificationMode.values()),
            randomFrom(SslClientAuthenticationMode.values()),
            DEFAULT_CIPHERS,
            SslConfigurationLoader.FIPS_APPROVED_PROTOCOLS
        );

        final Path dir = createTempDir();
        final Path file1 = dir.resolve(randomAlphaOfLength(1) + ".pem");
        final Path file2 = dir.resolve(randomAlphaOfLength(2) + ".pem");
        final Path file3 = dir.resolve(randomAlphaOfLength(3) + ".pem");
        final Path file4 = dir.resolve(randomAlphaOfLength(4) + ".pem");
        final Path file5 = dir.resolve(randomAlphaOfLength(5) + ".pem");

        Mockito.when(trustConfig.getDependentFiles()).thenReturn(Arrays.asList(file1, file2));
        Mockito.when(keyConfig.getDependentFiles()).thenReturn(Arrays.asList(file3, file4, file5));
        assertThat(configuration.getDependentFiles(), Matchers.containsInAnyOrder(file1, file2, file3, file4, file5));
    }

    public void testBuildSslContext() {
        final SslTrustConfig trustConfig = Mockito.mock(SslTrustConfig.class);
        final SslKeyConfig keyConfig = Mockito.mock(SslKeyConfig.class);
        final String protocol = randomFrom(SslConfigurationLoader.FIPS_APPROVED_PROTOCOLS);
        final SslConfiguration configuration = new SslConfiguration(
            trustConfig,
            keyConfig,
            randomFrom(SslVerificationMode.values()),
            randomFrom(SslClientAuthenticationMode.values()),
            DEFAULT_CIPHERS,
            Collections.singletonList(protocol)
        );

        Mockito.when(trustConfig.createTrustManager()).thenReturn(null);
        Mockito.when(keyConfig.createKeyManager()).thenReturn(null);
        final SSLContext sslContext = configuration.createSslContext();
        assertThat(sslContext.getProtocol(), equalTo(protocol));

        Mockito.verify(trustConfig).createTrustManager();
        Mockito.verify(keyConfig).createKeyManager();
        Mockito.verifyNoMoreInteractions(trustConfig, keyConfig);
    }

}
