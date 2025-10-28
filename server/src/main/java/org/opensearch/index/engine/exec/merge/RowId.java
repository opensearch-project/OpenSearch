/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

public class RowId {
    public long rowId;

    /**
     * We need this additional information around file name as well for post-processing usecases for reassigning row id.
     *
     * This file id can be the file name/id of the primary data format, all other data format needs to store this as well in doc.
     * Using this older filer name and older row id we can try getting new row id from rowIdMapping.
     */
    public String fileId;

    public RowId(long rowId, String fileId) {
        this.rowId = rowId;
        this.fileId = fileId;
    }
}
