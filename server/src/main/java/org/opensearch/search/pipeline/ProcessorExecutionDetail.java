/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
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
    public static final ParseField PROCESSOR_NAME_FIELD = new ParseField("processor_name");
    public static final ParseField DURATION_MILLIS_FIELD = new ParseField("duration_millis");
    public static final ParseField INPUT_DATA_FIELD = new ParseField("input_data");
    public static final ParseField OUTPUT_DATA_FIELD = new ParseField("output_data");

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
        builder.field(PROCESSOR_NAME_FIELD.getPreferredName(), processorName);
        builder.field(DURATION_MILLIS_FIELD.getPreferredName(), durationMillis);
        addFieldToXContent(builder, INPUT_DATA_FIELD.getPreferredName(), inputData, params);
        addFieldToXContent(builder, OUTPUT_DATA_FIELD.getPreferredName(), outputData, params);
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

    public static ProcessorExecutionDetail fromXContent(XContentParser parser) throws IOException {
        String processorName = null;
        long durationMillis = 0;
        Object inputData = null;
        Object outputData = null;

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            if (PROCESSOR_NAME_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                processorName = parser.text();
            } else if (DURATION_MILLIS_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                durationMillis = parser.longValue();
            } else if (INPUT_DATA_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                inputData = parseFieldFromXContent(parser);
            } else if (OUTPUT_DATA_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                outputData = parseFieldFromXContent(parser);
            } else {
                parser.skipChildren();
            }
        }

        if (processorName == null) {
            throw new IllegalArgumentException("Processor name is required");
        }

        return new ProcessorExecutionDetail(processorName, durationMillis, inputData, outputData);
    }

    private static Object parseFieldFromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_NULL) {
            return null;
        } else if (token == XContentParser.Token.START_ARRAY) {
            return parseArrayFromXContent(parser);
        } else if (token == XContentParser.Token.START_OBJECT) {
            return parser.map();
        } else {
            return parser.textOrNull();
        }
    }

    private static List<Object> parseArrayFromXContent(XContentParser parser) throws IOException {
        List<Object> list = new ArrayList<>();
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                list.add(parser.map());
            } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                list.add(parseArrayFromXContent(parser));
            } else {
                list.add(parser.textOrNull());
            }
        }
        return list;
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
