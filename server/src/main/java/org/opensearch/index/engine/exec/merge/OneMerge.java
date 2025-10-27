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

public class OneMerge {
    private final DataFormat dataFormat;
    private final Collection<FileMetadata> filesToMerge; // Files to merge as per given data format

    public OneMerge(DataFormat dataFormat, Collection<FileMetadata> filesToMerge) {
        this.dataFormat = dataFormat;
        this.filesToMerge = filesToMerge;
    }

    public DataFormat getDataFormat() {
        return dataFormat;
    }

    public Collection<FileMetadata> getFilesToMerge() {
        return filesToMerge;
    }

    public String toString() {
        return "Merge [dataFormat=" + dataFormat + ", filesToMerge=" + filesToMerge + "] ";
    }
}
