/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import java.util.Map;

public class RowIdMapping {

    Map<RowId, Long> mapping;
    private final String fileId;

    public RowIdMapping(Map<RowId, Long> mapping, String fileId) {
        this.mapping = mapping;
        this.fileId = fileId;
    }

    public long getNewRowId(RowId oldRowId) {
        return mapping.get(oldRowId);
    }
    public String getFileId() {
        return fileId;
    }
}
