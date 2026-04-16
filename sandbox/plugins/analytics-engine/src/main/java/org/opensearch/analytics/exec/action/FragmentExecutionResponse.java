/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Transport response carrying field names and result rows from a single shard scan execution.
 * <p>
 * This is the DATA-only subset of the former {@code FragmentExecutionResponse}. No payload-type
 * discriminator, no shuffle manifest, no broadcast handle, no metadata.
 * <p>
 * Each cell value is serialized via {@link StreamOutput#writeGenericValue(Object)} /
 * {@link StreamInput#readGenericValue()}, which handle common Java types
 * (String, Long, Double, Integer, null, byte[], etc.).
 * <p>
 * Wire format: {@code fieldNames (string list) + rowCount (vint) + per-row (colCount (vint) + cells)}.
 */
public class FragmentExecutionResponse extends ActionResponse {

    private final List<String> fieldNames;
    private final List<Object[]> rows;

    public FragmentExecutionResponse(List<String> fieldNames, List<Object[]> rows) {
        this.fieldNames = fieldNames;
        this.rows = rows;
    }

    public FragmentExecutionResponse(StreamInput in) throws IOException {
        super(in);
        this.fieldNames = in.readStringList();
        int rowCount = in.readVInt();
        this.rows = new ArrayList<>(rowCount);
        for (int r = 0; r < rowCount; r++) {
            int colCount = in.readVInt();
            Object[] row = new Object[colCount];
            for (int c = 0; c < colCount; c++) {
                row[c] = in.readGenericValue();
            }
            rows.add(row);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(fieldNames);
        out.writeVInt(rows.size());
        for (Object[] row : rows) {
            out.writeVInt(row.length);
            for (Object cell : row) {
                out.writeGenericValue(cell);
            }
        }
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public List<Object[]> getRows() {
        return rows;
    }
}
