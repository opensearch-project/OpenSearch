/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.tasks;

import org.opensearch.OpenSearchException;
import org.opensearch.client.Requests;
import org.opensearch.common.Nullable;
import org.opensearch.core.ParseField;
import org.opensearch.common.Strings;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.core.xcontent.InstantiatingObjectParser;
import org.opensearch.common.xcontent.ObjectParserHelper;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.opensearch.common.xcontent.XContentHelper.convertToMap;

/**
 * Information about a running task or a task that stored its result. Running tasks just have a {@link #getTask()} while
* tasks with stored result will have either a {@link #getError()} or {@link #getResponse()}.
*
* @opensearch.internal
*/
public final class ProtobufTaskResult implements ProtobufWriteable, ToXContentObject {
    private final boolean completed;
    private final ProtobufTaskInfo task;
    @Nullable
    private final BytesReference error;
    @Nullable
    private final BytesReference response;

    /**
     * Construct a {@linkplain TaskResult} for a task for which we don't have a result or error. That usually means that the task
    * is incomplete, but it could also mean that we waited for the task to complete but it didn't save any error information.
    */
    public ProtobufTaskResult(boolean completed, ProtobufTaskInfo task) {
        this(completed, task, null, null);
    }

    /**
     * Construct a {@linkplain TaskResult} for a task that completed with an error.
    */
    public ProtobufTaskResult(ProtobufTaskInfo task, Exception error) throws IOException {
        this(true, task, toXContent(error), null);
    }

    /**
     * Construct a {@linkplain ProtobufTaskResult} for a task that completed successfully.
    */
    public ProtobufTaskResult(ProtobufTaskInfo task, ToXContent response) throws IOException {
        this(true, task, null, XContentHelper.toXContent(response, Requests.INDEX_CONTENT_TYPE, true));
    }

    public ProtobufTaskResult(boolean completed, ProtobufTaskInfo task, @Nullable BytesReference error, @Nullable BytesReference result) {
        this.completed = completed;
        this.task = requireNonNull(task, "task is required");
        this.error = error;
        this.response = result;
    }

    /**
     * Read from a stream.
    */
    public ProtobufTaskResult(com.google.protobuf.CodedInputStream in) throws IOException {
        completed = in.readBool();
        task = new ProtobufTaskInfo(in);
        error = in.readOptionalBytesReference();
        response = in.readOptionalBytesReference();
    }

    @Override
    public void writeTo(com.google.protobuf.CodedOutputStream out) throws IOException {
        out.writeBool(0, completed);
        task.writeTo(out);
        out.writeByteArray(1, error);
        out.writeOptionalBytesReference(response);
    }

    /**
     * Get the task that this wraps.
    */
    public ProtobufTaskInfo getTask() {
        return task;
    }

    /**
     * Get the error that finished this task. Will return null if the task didn't finish with an error, it hasn't yet finished, or didn't
    * store its result.
    */
    public BytesReference getError() {
        return error;
    }

    /**
     * Convert {@link #getError()} from XContent to a Map for easy processing. Will return an empty map if the task didn't finish with an
    * error, hasn't yet finished, or didn't store its result.
    */
    public Map<String, Object> getErrorAsMap() {
        if (error == null) {
            return emptyMap();
        }
        return convertToMap(error, false).v2();
    }

    /**
     * Get the response that this task finished with. Will return null if the task was finished by an error, it hasn't yet finished, or
    * didn't store its result.
    */
    public BytesReference getResponse() {
        return response;
    }

    /**
     * Convert {@link #getResponse()} from XContent to a Map for easy processing. Will return an empty map if the task was finished with an
    * error, hasn't yet finished, or didn't store its result.
    */
    public Map<String, Object> getResponseAsMap() {
        if (response == null) {
            return emptyMap();
        }
        return convertToMap(response, false).v2();
    }

    public boolean isCompleted() {
        return completed;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder, params);
        return builder.endObject();
    }

    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("completed", completed);
        builder.startObject("task");
        task.toXContent(builder, params);
        builder.endObject();
        if (error != null) {
            XContentHelper.writeRawField("error", error, builder, params);
        }
        if (response != null) {
            XContentHelper.writeRawField("response", response, builder, params);
        }
        return builder;
    }

    public static final InstantiatingObjectParser<ProtobufTaskResult, Void> PARSER;

    static {
        InstantiatingObjectParser.Builder<ProtobufTaskResult, Void> parser = InstantiatingObjectParser.builder(
            "stored_task_result",
            true,
            ProtobufTaskResult.class
        );
        parser.declareBoolean(constructorArg(), new ParseField("completed"));
        parser.declareObject(constructorArg(), ProtobufTaskInfo.PARSER, new ParseField("task"));
        ObjectParserHelper<ProtobufTaskResult, Void> parserHelper = new ObjectParserHelper<>();
        parserHelper.declareRawObject(parser, optionalConstructorArg(), new ParseField("error"));
        parserHelper.declareRawObject(parser, optionalConstructorArg(), new ParseField("response"));
        PARSER = parser.build();
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this);
    }

    // Implements equals and hashcode for testing
    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != ProtobufTaskResult.class) {
            return false;
        }
        ProtobufTaskResult other = (ProtobufTaskResult) obj;
        /*
        * Equality of error and result is done by converting them to a map first. Not efficient but ignores field order and spacing
        * differences so perfect for testing.
        */
        return Objects.equals(completed, other.completed)
            && Objects.equals(task, other.task)
            && Objects.equals(getErrorAsMap(), other.getErrorAsMap())
            && Objects.equals(getResponseAsMap(), other.getResponseAsMap());
    }

    @Override
    public int hashCode() {
        /*
        * Hashing of error and result is done by converting them to a map first. Not efficient but ignores field order and spacing
        * differences so perfect for testing.
        */
        return Objects.hash(completed, task, getErrorAsMap(), getResponseAsMap());
    }

    private static BytesReference toXContent(Exception error) throws IOException {
        try (XContentBuilder builder = XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE)) {
            builder.startObject();
            OpenSearchException.generateThrowableXContent(builder, ToXContent.EMPTY_PARAMS, error);
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }
}
