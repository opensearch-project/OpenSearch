/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

public class FileStats {

    private final long size;
    private final long docCount;

    public FileStats(long size, long docCount) {
        this.size = size;
        this.docCount = docCount;
    }

    public long getSize() {
        return size;
    }

    public long getDocCount() {
        return docCount;
    }

    @Override
    public String toString() {
        return "FileStats{" + "size=" + size + ", docCount=" + docCount + '}';
    }
}
