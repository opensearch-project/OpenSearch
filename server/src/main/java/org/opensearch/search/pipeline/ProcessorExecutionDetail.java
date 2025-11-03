/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.XContentUtils;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Detailed information about a processor execution in a search pipeline.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.19.0")
public class ProcessorExecutionDetail implements Writeable, ToXContentObject {

    private final String processorName;
    private long durationMillis;
    private Object inputData;
    private Object outputData;
    private ProcessorStatus status;
    private String errorMessage;
    private String tag;
    private static final ParseField PROCESSOR_NAME_FIELD = new ParseField("processor_name");
    private static final ParseField DURATION_MILLIS_FIELD = new ParseField("duration_millis");
    private static final ParseField INPUT_DATA_FIELD = new ParseField("input_data");
    private static final ParseField OUTPUT_DATA_FIELD = new ParseField("output_data");
    private static final ParseField STATUS_FIELD = new ParseField("status");
    private static final ParseField ERROR_MESSAGE_FIELD = new ParseField("error");
    private static final ParseField TAG_FIELD = new ParseField("tag");
    // Key for processor execution details
    public static final String PROCESSOR_EXECUTION_DETAILS_KEY = "processorExecutionDetails";

    /**
     * Constructor for ProcessorExecutionDetail
     */
    public ProcessorExecutionDetail(
        String processorName,
        long durationMillis,
        Object inputData,
        Object outputData,
        ProcessorStatus status,
        String errorMessage,
        String tag
    ) {
        this.processorName = processorName;
        this.durationMillis = durationMillis;
        this.inputData = inputData;
        this.outputData = outputData;
        this.status = status;
        this.errorMessage = errorMessage;
        this.tag = tag;
    }

    public ProcessorExecutionDetail(String processorName) {
        this(processorName, 0, null, null, ProcessorStatus.SUCCESS, null, null);
    }

    public ProcessorExecutionDetail(String processorName, String tag) {
        this(processorName, 0, null, null, ProcessorStatus.SUCCESS, null, tag);
    }

    public ProcessorExecutionDetail(StreamInput in) throws IOException {
        this.processorName = in.readString();
        this.durationMillis = in.readLong();
        this.inputData = in.readGenericValue();
        this.outputData = in.readGenericValue();
        this.status = in.readEnum(ProcessorStatus.class);
        this.errorMessage = in.readString();
        this.tag = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(processorName);
        out.writeLong(durationMillis);
        out.writeGenericValue(inputData);
        out.writeGenericValue(outputData);
        out.writeEnum(status);
        out.writeString(errorMessage);
        out.writeString(tag);
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

    public void markProcessorAsFailed(ProcessorStatus status, String errorMessage) {
        this.status = status;
        this.errorMessage = errorMessage;
    }

    public ProcessorStatus getStatus() {
        return status;
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

    /**
     * Serializes the processor execution detail into XContent.
     * Includes the error message only if the processor has failed.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(PROCESSOR_NAME_FIELD.getPreferredName(), processorName);
        // tag is optional when setting up processor
        if (tag != null) {
            builder.field(TAG_FIELD.getPreferredName(), tag);
        }
        builder.field(DURATION_MILLIS_FIELD.getPreferredName(), durationMillis);
        builder.field(STATUS_FIELD.getPreferredName(), status.name().toLowerCase(Locale.ROOT));
        if (status == ProcessorStatus.FAIL) {
            builder.field(ERROR_MESSAGE_FIELD.getPreferredName(), errorMessage);
        }
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

        if (data instanceof List<?> list) {
            builder.startArray(fieldName);
            for (Object item : list) {
                writeItemToXContent(builder, item, params);
            }
            builder.endArray();
        } else if (data instanceof Map<?, ?> map) {
            builder.startObject(fieldName);
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                addFieldToXContent(builder, entry.getKey().toString(), entry.getValue(), params);
            }
            builder.endObject();
        } else if (data instanceof ToXContentObject toXContentObject) {
            builder.field(fieldName);
            toXContentObject.toXContent(builder, params);
        } else if (data instanceof String jsonString) {
            // If the data is a String, attempt to parse it as JSON
            try {
                // check if its json string
                try (
                    XContentParser parser = XContentType.JSON.xContent()
                        .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, jsonString)
                ) {
                    Map<String, Object> parsedMap = parser.map();

                    builder.startObject(fieldName);
                    for (Map.Entry<String, Object> entry : parsedMap.entrySet()) {
                        addFieldToXContent(builder, entry.getKey(), entry.getValue(), params);
                    }
                    builder.endObject();
                }
            } catch (IOException e) {
                // If parsing fails, write the string as a plain field
                builder.field(fieldName, jsonString);
            }
        } else {
            builder.field(fieldName, data);
        }
    }

    private void writeItemToXContent(XContentBuilder builder, Object item, Params params) throws IOException {
        if (item instanceof ToXContentObject toXContentObject) {
            toXContentObject.toXContent(builder, params);
        } else {
            builder.value(item);
        }
    }

    public static ProcessorExecutionDetail fromXContent(XContentParser parser) throws IOException {
        String processorName = null;
        long durationMillis = 0;
        Object inputData = null;
        Object outputData = null;
        ProcessorStatus status = null;
        String errorMessage = null;
        String tag = null;
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            parser.nextToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        }
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            if (PROCESSOR_NAME_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                processorName = parser.text();
            } else if (TAG_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                tag = parser.text();
            } else if (DURATION_MILLIS_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                durationMillis = parser.longValue();
            } else if (STATUS_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                status = ProcessorStatus.valueOf(parser.text().toUpperCase(Locale.ROOT));
            } else if (ERROR_MESSAGE_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                errorMessage = parser.text();
            } else if (INPUT_DATA_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                inputData = XContentUtils.readValue(parser, parser.currentToken());
            } else if (OUTPUT_DATA_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                outputData = XContentUtils.readValue(parser, parser.currentToken());
            } else {
                parser.skipChildren();
            }
        }

        if (processorName == null) {
            throw new IllegalArgumentException("Processor name is required");
        }

        return new ProcessorExecutionDetail(processorName, durationMillis, inputData, outputData, status, errorMessage, tag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processorName, durationMillis, inputData, outputData, status, errorMessage, tag);
    }

    /**
     * Represents the status of a processor in the search pipeline.
     *
     * <p>This enum is used to indicate whether a processor has executed successfully
     * or encountered a failure during its execution. It helps in categorizing the
     * execution result of processors within a pipeline.
     *
     */
    @PublicApi(since = "2.19.0")
    public enum ProcessorStatus {
        SUCCESS,
        FAIL
    }
}
