/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.FileMetadata;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class MergeResult {

    private RowIdMapping rowIdMapping;

    private Map<DataFormat, Collection<FileMetadata>> mergedFileMetadata = new HashMap<>();

    public MergeResult(RowIdMapping rowIdMapping, Map<DataFormat, Collection<FileMetadata>> mergedFileMetadata) {
        this.rowIdMapping = rowIdMapping;
        this.mergedFileMetadata = mergedFileMetadata;
    }

    public RowIdMapping getRowIdMapping() {
        return rowIdMapping;
    }

    public Map<DataFormat, Collection<FileMetadata>> getMergedFileMetadata() {
        return mergedFileMetadata;
    }
}
