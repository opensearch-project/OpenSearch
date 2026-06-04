/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import org.opensearch.test.OpenSearchTestCase;

public class ConfigurationPropertiesTests extends OpenSearchTestCase {

    public void testToStringWithPasswordSet() {
        // given
        var config = new ConfigurationProperties("/path/to/truststore.bcfks", "BCFKS", "p@ssw0rd!", "BCFIPS");

        // when
        String output = config.toString();

        // then
        assertTrue(output.contains("javax.net.ssl.trustStore: /path/to/truststore.bcfks"));
        assertTrue(output.contains("javax.net.ssl.trustStoreType: BCFKS"));
        assertTrue(output.contains("javax.net.ssl.trustStoreProvider: BCFIPS"));
        assertTrue(output.contains("javax.net.ssl.trustStorePassword: [SET]"));
        assertFalse(output.contains("changeit"));
    }

    public void testToStringWithEmptyPassword() {
        // given
        var config = new ConfigurationProperties("/path/to/truststore.jks", "JKS", "", "SUN");

        // when
        String output = config.toString();

        // then
        assertTrue(output.contains("javax.net.ssl.trustStore: /path/to/truststore.jks"));
        assertTrue(output.contains("javax.net.ssl.trustStoreType: JKS"));
        assertTrue(output.contains("javax.net.ssl.trustStoreProvider: SUN"));
        assertTrue(output.contains("javax.net.ssl.trustStorePassword: [NOT SET]"));
    }
}
