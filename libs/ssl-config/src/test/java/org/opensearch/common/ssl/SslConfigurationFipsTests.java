/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.ssl;

import java.util.Collections;
import java.util.List;

import org.mockito.Mockito;

import static org.opensearch.common.ssl.SslConfigurationLoader.DEFAULT_CIPHERS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SslConfigurationFipsTests extends SslConfigurationTests {

    public void testCreateSslContextWithUnsupportedProtocols() {
        final SslTrustConfig trustConfig = Mockito.mock(SslTrustConfig.class);
        final SslKeyConfig keyConfig = Mockito.mock(SslKeyConfig.class);
        SslConfiguration configuration = new SslConfiguration(
            trustConfig,
            keyConfig,
            randomFrom(SslVerificationMode.values()),
            randomFrom(SslClientAuthenticationMode.values()),
            DEFAULT_CIPHERS,
            Collections.singletonList("DTLSv1.2")
        );

        Exception e = assertThrows(SslConfigException.class, configuration::createSslContext);
        assertThat(e.getMessage(), containsString("in FIPS mode only the following SSL/TLS protocols are allowed: [TLSv1.3, TLSv1.2]"));
    }

    public void testNotSupportedProtocolsInFipsJvm() {
        final SslTrustConfig trustConfig = Mockito.mock(SslTrustConfig.class);
        final SslKeyConfig keyConfig = Mockito.mock(SslKeyConfig.class);

        {
            var sslConfiguration = new SslConfiguration(
                trustConfig,
                keyConfig,
                randomFrom(SslVerificationMode.values()),
                randomFrom(SslClientAuthenticationMode.values()),
                DEFAULT_CIPHERS,
                List.of("TLSv1.2")
            );
            sslConfiguration.createSslContext();
        }

        {
            final String nonFipsProtocols = randomFrom(List.of("TLSv1.1", "TLSv1", "SSLv3", "SSLv2Hello", "SSLv2"));
            var sslConfiguration = new SslConfiguration(
                trustConfig,
                keyConfig,
                randomFrom(SslVerificationMode.values()),
                randomFrom(SslClientAuthenticationMode.values()),
                DEFAULT_CIPHERS,
                Collections.singletonList(nonFipsProtocols)
            );
            var exception = assertThrows(
                "cannot create SSL/TLS context without any supported protocols",
                SslConfigException.class,
                sslConfiguration::createSslContext
            );
            assertThat(
                exception.getMessage(),
                equalTo(
                    "in FIPS mode only the following SSL/TLS protocols are allowed: [TLSv1.3, TLSv1.2]. This issue may be caused by the 'ssl.supported_protocols' setting."
                )
            );
        }
    }
}
