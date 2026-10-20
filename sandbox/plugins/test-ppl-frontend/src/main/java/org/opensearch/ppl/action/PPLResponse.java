/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.opensearch.analytics.exec.profile.QueryProfile;
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
 * from the unified PPL query execution pipeline. Optionally carries
 * a {@link QueryProfile} when the request was issued via the explain endpoint.
 */
public class PPLResponse extends ActionResponse implements ToXContentObject {

    private final List<String> columns;
    private final List<Object[]> rows;
    private final QueryProfile profile;

    public PPLResponse(List<String> columns, List<Object[]> rows) {
        this(columns, rows, null);
    }

    public PPLResponse(List<String> columns, List<Object[]> rows, QueryProfile profile) {
        this.columns = columns;
        this.rows = rows;
        this.profile = profile;
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
        // Profile is not serialized over transport — it's only used in the local response path.
        this.profile = null;
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

    public QueryProfile getProfile() {
        return profile;
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
        if (profile != null) {
            builder.field("profile");
            profile.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }
}
