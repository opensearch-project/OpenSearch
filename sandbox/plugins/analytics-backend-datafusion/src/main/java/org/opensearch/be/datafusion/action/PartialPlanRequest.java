/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.be.datafusion.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request carrying a SQL query and parquet path for data-node execution.
 */
public class PartialPlanRequest extends ActionRequest {

    private final String parquetPath;
    private final String sql;

    public PartialPlanRequest(String parquetPath, String sql) {
        this.parquetPath = parquetPath;
        this.sql = sql;
    }

    public PartialPlanRequest(StreamInput in) throws IOException {
        super(in);
        this.parquetPath = in.readString();
        this.sql = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(parquetPath);
        out.writeString(sql);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String getParquetPath() {
        return parquetPath;
    }

    public String getSql() {
        return sql;
    }
}
