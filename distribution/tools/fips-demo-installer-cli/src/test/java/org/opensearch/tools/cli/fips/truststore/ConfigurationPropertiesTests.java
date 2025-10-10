/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import org.opensearch.test.OpenSearchTestCase;

import java.io.StringWriter;

public class ConfigurationPropertiesTests extends OpenSearchTestCase {

    public void testLogoutWithPasswordSet() {
        // given
        var config = new ConfigurationProperties("/path/to/truststore.bcfks", "BCFKS", "p@ssw0rd!", "BCFIPS");

        // when
        StringWriter result = config.logout();

        // then
        String output = result.toString();
        assertTrue(output.contains("javax.net.ssl.trustStore: /path/to/truststore.bcfks"));
        assertTrue(output.contains("javax.net.ssl.trustStoreType: BCFKS"));
        assertTrue(output.contains("javax.net.ssl.trustStoreProvider: BCFIPS"));
        assertTrue(output.contains("javax.net.ssl.trustStorePassword: [SET]"));
        assertFalse(output.contains("changeit"));
    }

    public void testLogoutWithEmptyPassword() {
        // given
        var config = new ConfigurationProperties("/path/to/truststore.jks", "JKS", "", "SUN");

        // when
        StringWriter result = config.logout();

        // then
        String output = result.toString();
        assertTrue(output.contains("javax.net.ssl.trustStore: /path/to/truststore.jks"));
        assertTrue(output.contains("javax.net.ssl.trustStoreType: JKS"));
        assertTrue(output.contains("javax.net.ssl.trustStoreProvider: SUN"));
        assertTrue(output.contains("javax.net.ssl.trustStorePassword: [NOT SET]"));
    }
}
