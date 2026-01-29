/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.example.stream.benchmark;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request for benchmark stream action
 */
public class BenchmarkStreamRequest extends ActionRequest {

    private int rows = 100;
    private int columns = 10;
    private int avgColumnLength = 100;
    private String columnType = "string";
    private boolean useStreamTransport = true;
    private int parallelRequests = 1;
    private int totalRequests = 0;
    private int targetTps = 0;
    private String threadPool = "generic";
    private int batchSize = 100;

    /** Constructor */
    public BenchmarkStreamRequest() {}

    /**
     * Constructor from stream input
     * @param in stream input
     * @throws IOException if an I/O error occurs
     */
    public BenchmarkStreamRequest(StreamInput in) throws IOException {
        super(in);
        rows = in.readVInt();
        columns = in.readVInt();
        avgColumnLength = in.readVInt();
        columnType = in.readString();
        useStreamTransport = in.readBoolean();
        parallelRequests = in.readVInt();
        totalRequests = in.readVInt();
        targetTps = in.readVInt();
        threadPool = in.readString();
        batchSize = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(rows);
        out.writeVInt(columns);
        out.writeVInt(avgColumnLength);
        out.writeString(columnType);
        out.writeBoolean(useStreamTransport);
        out.writeVInt(parallelRequests);
        out.writeVInt(totalRequests);
        out.writeVInt(targetTps);
        out.writeString(threadPool);
        out.writeVInt(batchSize);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (rows <= 0) {
            validationException = addValidationError("rows must be > 0", validationException);
        }
        if (columns <= 0) {
            validationException = addValidationError("columns must be > 0", validationException);
        }
        if (parallelRequests <= 0) {
            validationException = addValidationError("parallel_requests must be > 0", validationException);
        }
        if (batchSize <= 0) {
            validationException = addValidationError("batch_size must be > 0", validationException);
        }
        return validationException;
    }

    private ActionRequestValidationException addValidationError(String error, ActionRequestValidationException exception) {
        if (exception == null) {
            exception = new ActionRequestValidationException();
        }
        exception.addValidationError(error);
        return exception;
    }

    int getRows() {
        return rows;
    }

    void setRows(int rows) {
        this.rows = rows;
    }

    int getColumns() {
        return columns;
    }

    void setColumns(int columns) {
        this.columns = columns;
    }

    int getAvgColumnLength() {
        return avgColumnLength;
    }

    void setAvgColumnLength(int avgColumnLength) {
        this.avgColumnLength = avgColumnLength;
    }

    String getColumnType() {
        return columnType;
    }

    void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    boolean isUseStreamTransport() {
        return useStreamTransport;
    }

    void setUseStreamTransport(boolean useStreamTransport) {
        this.useStreamTransport = useStreamTransport;
    }

    int getParallelRequests() {
        return parallelRequests;
    }

    void setParallelRequests(int parallelRequests) {
        this.parallelRequests = parallelRequests;
    }

    int getTotalRequests() {
        return totalRequests;
    }

    void setTotalRequests(int totalRequests) {
        this.totalRequests = totalRequests;
    }

    int getTargetTps() {
        return targetTps;
    }

    void setTargetTps(int targetTps) {
        this.targetTps = targetTps;
    }

    String getThreadPool() {
        return threadPool;
    }

    void setThreadPool(String threadPool) {
        this.threadPool = threadPool;
    }

    int getBatchSize() {
        return batchSize;
    }

    void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
