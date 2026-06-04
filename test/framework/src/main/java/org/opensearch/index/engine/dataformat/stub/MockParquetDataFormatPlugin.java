/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.stub;

import java.util.Set;

/**
 * A mock {@link MockDataFormatPlugin} that registers "parquet" as a data format for testing.
 */
public class MockParquetDataFormatPlugin extends MockDataFormatPlugin {

    public MockParquetDataFormatPlugin() {
        super(new MockDataFormat("parquet", 100L, Set.of()));
    }
}
