/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.util.HashMap;
import java.util.Map;

public class MergeResult {

    private final RowIdMapping rowIdMapping;

    private Map<DataFormat, WriterFileSet> mergedWriterFileSet;

    public MergeResult(RowIdMapping rowIdMapping, Map<DataFormat, WriterFileSet> mergedWriterFileSet) {
        this.rowIdMapping = rowIdMapping;
        this.mergedWriterFileSet = mergedWriterFileSet;
    }

    public RowIdMapping getRowIdMapping() {
        return rowIdMapping;
    }

    public Map<DataFormat, WriterFileSet> getMergedWriterFileSet () {
        return mergedWriterFileSet;
    }

    public WriterFileSet getMergedWriterFileSetForDataformat (DataFormat dataFormat) {
        return mergedWriterFileSet.get(dataFormat);
    }

    @Override
    public String toString() {
        return "MergeResult{" +
            "rowIdMapping=" + rowIdMapping +
            ", mergedWriterFileSet=" + mergedWriterFileSet +
            '}';
    }
}
