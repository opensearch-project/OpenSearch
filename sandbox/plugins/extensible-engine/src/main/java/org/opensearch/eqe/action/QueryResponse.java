/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.eqe.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Response from coordinator-level query execution.
 * Contains column names and rows of values aggregated from all data nodes.
 */
public class QueryResponse extends ActionResponse {

    private final List<String> columns;
    private final List<Object[]> rows;
    private final String error;

    public QueryResponse(List<String> columns, List<Object[]> rows) {
        this.columns = columns;
        this.rows = rows;
        this.error = null;
    }

    public QueryResponse(String error) {
        this.columns = List.of();
        this.rows = List.of();
        this.error = error;
    }

    public QueryResponse(StreamInput in) throws IOException {
        super(in);
        this.columns = in.readStringList();
        int rowCount = in.readVInt();
        this.rows = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            int colCount = in.readVInt();
            Object[] row = new Object[colCount];
            for (int j = 0; j < colCount; j++) {
                row[j] = in.readGenericValue();
            }
            rows.add(row);
        }
        this.error = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(columns);
        out.writeVInt(rows.size());
        for (Object[] row : rows) {
            out.writeVInt(row.length);
            for (Object val : row) {
                out.writeGenericValue(val);
            }
        }
        out.writeOptionalString(error);
    }

    public List<String> getColumns() { return columns; }
    public List<Object[]> getRows() { return rows; }
    public String getError() { return error; }
    public boolean hasError() { return error != null; }
}
