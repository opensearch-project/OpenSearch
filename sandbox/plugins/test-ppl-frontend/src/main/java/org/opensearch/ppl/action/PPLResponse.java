/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Transport-layer response carrying column names and result rows
 * from the unified PPL query execution pipeline.
 */
public class PPLResponse extends ActionResponse implements ToXContentObject {

    private final List<String> columns;
    private final List<Object[]> rows;

    public PPLResponse(List<String> columns, List<Object[]> rows) {
        this.columns = columns;
        this.rows = rows;
    }

    public PPLResponse(StreamInput in) throws IOException {
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
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<Object[]> getRows() {
        return rows;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.startArray("columns");
        for (String col : columns) {
            builder.value(col);
        }
        builder.endArray();
        builder.startArray("rows");
        for (Object[] row : rows) {
            builder.startArray();
            for (Object val : row) {
                builder.value(val);
            }
            builder.endArray();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
