/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.opensearch.index.engine.exec.WriterFileSet;
import java.util.List;

public class MergeInput {
    private final List<WriterFileSet> fileMetadataList;
    private final long writerGeneration;
    private final String sortingField;
    private final boolean reverseSort;
    private final String indexName;

    public MergeInput(List<WriterFileSet> fileMetadataList, long writerGeneration, String sortingField, boolean reverseSort, String indexName) {
        this.fileMetadataList = fileMetadataList;
        this.writerGeneration = writerGeneration;
        this.sortingField = sortingField;
        this.reverseSort = reverseSort;
        this.indexName = indexName;
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

    public String getIndexName() {
        return indexName;
    }
}
