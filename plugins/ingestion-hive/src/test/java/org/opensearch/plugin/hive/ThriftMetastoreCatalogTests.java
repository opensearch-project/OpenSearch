/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.hive;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ThriftMetastoreCatalogTests extends OpenSearchTestCase {

    private ThriftMetastoreCatalog createCatalog(String metastoreUri) {
        Map<String, Object> params = new HashMap<>();
        params.put("metastore_uri", metastoreUri);
        params.put("database", "db");
        params.put("table", "tbl");
        return new ThriftMetastoreCatalog(new HiveSourceConfig(params, 1));
    }

    public void testConnectRejectsUriWithoutPort() {
        IOException e = expectThrows(IOException.class, () -> createCatalog("thrift://metastore-host").connect());
        assertTrue(e.getMessage(), e.getMessage().contains("Expected format: thrift://host:port"));
    }

    public void testConnectRejectsUriWithEmptyHost() {
        IOException e = expectThrows(IOException.class, () -> createCatalog("thrift://:9083").connect());
        assertTrue(e.getMessage(), e.getMessage().contains("Expected format: thrift://host:port"));
    }

    public void testConnectRejectsNonNumericPort() {
        // java.net.URI treats an authority with a non-numeric port as having no
        // host, so this fails the same host/port validation as the other cases.
        IOException e = expectThrows(IOException.class, () -> createCatalog("thrift://host:port").connect());
        assertTrue(e.getMessage(), e.getMessage().contains("Expected format: thrift://host:port"));
    }

    public void testConnectRejectsWrongScheme() {
        IOException e = expectThrows(IOException.class, () -> createCatalog("http://host:9083").connect());
        assertTrue(e.getMessage(), e.getMessage().contains("Expected format: thrift://host:port"));
    }

    public void testParseMetastoreUriAcceptsHostPort() throws Exception {
        java.net.URI parsed = ThriftMetastoreCatalog.parseMetastoreUri("thrift://metastore-host:9083");
        assertEquals("metastore-host", parsed.getHost());
        assertEquals(9083, parsed.getPort());
    }

    public void testParseMetastoreUriAcceptsIpv6Literal() throws Exception {
        java.net.URI parsed = ThriftMetastoreCatalog.parseMetastoreUri("thrift://[::1]:9083");
        assertEquals("[::1]", parsed.getHost());
        assertEquals(9083, parsed.getPort());
    }
}
