/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import java.util.List;

public class MergeInput {
    private List<WriterFileSet> fileMetadataList;
    private long writerGeneration;
    private String sortingField;
    private boolean reverseSort;

    public MergeInput(List<WriterFileSet> fileMetadataList, long writerGeneration, String sortingField, boolean reverseSort) {
        this.fileMetadataList = fileMetadataList;
        this.writerGeneration = writerGeneration;
        this.sortingField = sortingField;
        this.reverseSort = reverseSort;
    }

    public List<WriterFileSet> getFileMetadataList() {
        return fileMetadataList;
    }

    public long getWriterGeneration() {
        return writerGeneration;
    }

    public String getSortingField() {
        return sortingField;
    }

    public boolean isReverseSort() {
        return reverseSort;
    }
}
