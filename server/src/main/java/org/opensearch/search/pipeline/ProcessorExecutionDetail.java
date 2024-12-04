/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Detailed information about a processor execution in a search pipeline.
 *
 * @opensearch.internal
 */
@PublicApi(since = "1.0.0")
public class ProcessorExecutionDetail implements Writeable, ToXContentObject {

    private final String processorName;
    private long durationMillis;
    private Object inputData;
    private Object outputData;
    private static final Logger logger = LogManager.getLogger(ProcessorExecutionDetail.class);

    /**
     * Constructor for ProcessorExecutionDetail
     */
    public ProcessorExecutionDetail(String processorName, long durationMillis, Object inputData, Object outputData) {
        this.processorName = processorName;
        this.durationMillis = durationMillis;
        this.inputData = inputData;
        this.outputData = outputData;
    }

    public ProcessorExecutionDetail(String processorName) {
        this(processorName, 0, null, null);

    }

    public ProcessorExecutionDetail(StreamInput in) throws IOException {
        this.processorName = in.readString();
        this.durationMillis = in.readLong();
        this.inputData = in.readGenericValue();
        this.outputData = in.readGenericValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(processorName);
        out.writeLong(durationMillis);
        out.writeGenericValue(inputData);
        out.writeGenericValue(outputData);
    }

    public String getProcessorName() {
        return processorName;
    }

    public long getDurationMillis() {
        return durationMillis;
    }

    public Object getInputData() {
        return inputData;
    }

    public Object getOutputData() {
        return outputData;
    }

    /**
     * Adds or updates the input data for this processor execution detail.
     *
     * @param inputData the new input data
     */
    public void addInput(Object inputData) {
        this.inputData = inputData;
    }

    /**
     * Adds or updates the output data for this processor execution detail.
     *
     * @param outputData the new output data
     */
    public void addOutput(Object outputData) {
        this.outputData = outputData;
    }

    /**
     * Adds or updates the duration of the processor execution.
     *
     * @param durationMillis the new duration in milliseconds
     */
    public void addTook(long durationMillis) {
        this.durationMillis = durationMillis;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("processor_name", processorName);
        builder.field("duration_millis", durationMillis);

        addFieldToXContent(builder, "input_data", inputData, params);

        addFieldToXContent(builder, "output_data", outputData, params);

        builder.endObject();
        return builder;
    }

    private void addFieldToXContent(XContentBuilder builder, String fieldName, Object data, Params params) throws IOException {
        if (data == null) {
            builder.nullField(fieldName);
            return;
        }

        if (data instanceof List) {
            builder.startArray(fieldName);
            for (Object item : (List<?>) data) {
                writeItemToXContent(builder, item, params);
            }
            builder.endArray();
        } else if (data instanceof ToXContentObject) {
            builder.field(fieldName);
            ((ToXContentObject) data).toXContent(builder, params);
        } else {
            builder.field(fieldName, data);
        }
    }

    private void writeItemToXContent(XContentBuilder builder, Object item, Params params) throws IOException {
        if (item instanceof ToXContentObject) {
            ((ToXContentObject) item).toXContent(builder, params);
        } else {
            builder.value(item);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProcessorExecutionDetail that = (ProcessorExecutionDetail) o;
        return durationMillis == that.durationMillis
            && Objects.equals(processorName, that.processorName)
            && Objects.equals(inputData, that.inputData)
            && Objects.equals(outputData, that.outputData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processorName, durationMillis, inputData, outputData);
    }

    @Override
    public String toString() {
        return "ProcessorExecutionDetail{"
            + "processorName='"
            + processorName
            + '\''
            + ", durationMillis="
            + durationMillis
            + ", inputData="
            + inputData
            + ", outputData="
            + outputData
            + '}';
    }
}
