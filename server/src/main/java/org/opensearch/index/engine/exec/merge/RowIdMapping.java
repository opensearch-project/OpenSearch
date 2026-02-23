/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public final class RowIdMapping {

    private final Map<RowId, Long> mapping;
    private final String fileId;

    public RowIdMapping(Map<RowId, Long> mapping, String fileId) {
        this.mapping = Collections.unmodifiableMap(Objects.requireNonNull(mapping, "mapping cannot be null"));
        this.fileId = Objects.requireNonNull(fileId, "fileId cannot be null");
    }

    public long getNewRowId(RowId oldRowId) {
        return mapping.getOrDefault(oldRowId, -1L);
    }
    public String getFileId() {
        return fileId;
    }

    @Override
    public String toString() {
        return "RowIdMapping{" +
            "mapping=" + mapping +
            ", fileId='" + fileId + '\'' +
            '}';
    }
}
