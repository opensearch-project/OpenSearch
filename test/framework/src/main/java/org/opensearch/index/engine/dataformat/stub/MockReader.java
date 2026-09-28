/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.stub;

import java.util.List;

/**
 * A mock reader for testing purposes.
 */
public class MockReader {
    public final List<String> fileNames;
    public final long totalRows;
    public boolean closed;

    public MockReader(List<String> fileNames, long totalRows) {
        this.fileNames = fileNames;
        this.totalRows = totalRows;
    }

    public void close() {
        closed = true;
    }
}
