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
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Response from shard-level plan fragment execution.
 */
public class ShardQueryResponse extends ActionResponse {

    private final ShardId shardId;
    private final List<String> columns;
    private final List<Object[]> rows;
    private final String error;

    public ShardQueryResponse(ShardId shardId, List<String> columns, List<Object[]> rows) {
        this.shardId = shardId;
        this.columns = columns;
        this.rows = rows;
        this.error = null;
    }

    public ShardQueryResponse(ShardId shardId, String error) {
        this.shardId = shardId;
        this.columns = List.of();
        this.rows = List.of();
        this.error = error;
    }

    public ShardQueryResponse(StreamInput in) throws IOException {
        super(in);
        this.shardId = new ShardId(in);
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
        shardId.writeTo(out);
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

    public ShardId getShardId() { return shardId; }
    public List<String> getColumns() { return columns; }
    public List<Object[]> getRows() { return rows; }
    public String getError() { return error; }
    public boolean hasError() { return error != null; }
}
