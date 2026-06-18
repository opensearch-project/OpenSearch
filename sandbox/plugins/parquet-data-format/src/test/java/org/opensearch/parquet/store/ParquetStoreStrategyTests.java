/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.store;

import org.opensearch.index.engine.dataformat.DataFormatStoreHandlerFactory;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Optional;

/**
 * Unit tests for {@link ParquetStoreStrategy}.
 */
public class ParquetStoreStrategyTests extends OpenSearchTestCase {

    public void testStoreHandlerReturnsFactory() {
        ParquetStoreStrategy strategy = new ParquetStoreStrategy();
        Optional<DataFormatStoreHandlerFactory> factory = strategy.storeHandler();
        assertTrue("storeHandler() should return a present Optional", factory.isPresent());
        assertNotNull("Factory should not be null", factory.get());
    }

    public void testOwnsParquetFiles() {
        ParquetStoreStrategy strategy = new ParquetStoreStrategy();
        assertTrue(strategy.owns("parquet", "parquet/_0.parquet"));
        assertTrue(strategy.owns("parquet", "parquet/seg_1.parquet"));
    }

    public void testDoesNotOwnLuceneFiles() {
        ParquetStoreStrategy strategy = new ParquetStoreStrategy();
        assertFalse(strategy.owns("parquet", "_0.cfe"));
        assertFalse(strategy.owns("parquet", "segments_1"));
    }

    public void testDoesNotOwnNullFile() {
        ParquetStoreStrategy strategy = new ParquetStoreStrategy();
        assertFalse(strategy.owns("parquet", null));
    }

    public void testRemotePathDefault() {
        ParquetStoreStrategy strategy = new ParquetStoreStrategy();
        // basePath from getRemoteBasePath("parquet") already includes the format subdirectory
        String remotePath = strategy.remotePath("parquet", "base/path/parquet/", "parquet/_0.parquet", "_0.parquet__UUID1");
        assertEquals("base/path/parquet/_0.parquet__UUID1", remotePath);
    }

    public void testRemotePathEmptyBasePath() {
        ParquetStoreStrategy strategy = new ParquetStoreStrategy();
        String remotePath = strategy.remotePath("parquet", "", "parquet/_0.parquet", "_0.parquet__UUID1");
        assertEquals("_0.parquet__UUID1", remotePath);
    }
}
