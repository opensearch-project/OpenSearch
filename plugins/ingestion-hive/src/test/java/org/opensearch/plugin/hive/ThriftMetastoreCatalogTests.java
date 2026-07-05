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
        IOException e = expectThrows(IOException.class, () -> createCatalog("thrift://host:port").connect());
        assertTrue(e.getMessage(), e.getMessage().contains("Invalid port"));
    }
}
