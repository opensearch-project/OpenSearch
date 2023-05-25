/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*
* Modifications Copyright OpenSearch Contributors. See
* GitHub history for details.
*/

package org.opensearch.tasks;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.OpenSearchException;
import org.opensearch.client.Requests;
import org.opensearch.common.Nullable;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;

import java.io.IOException;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
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

    private ProtobufStreamInput protobufStreamInput;

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
    public ProtobufTaskResult(CodedInputStream in) throws IOException {
        protobufStreamInput = new ProtobufStreamInput(in);
        completed = in.readBool();
        task = new ProtobufTaskInfo(in);
        error = protobufStreamInput.readOptionalBytesReference();
        response = protobufStreamInput.readOptionalBytesReference();
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        out.writeBoolNoTag(completed);
        task.writeTo(out);
        out.writeByteArrayNoTag(BytesReference.toBytes(error));
        out.writeByteArrayNoTag(BytesReference.toBytes(response));
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

    private static BytesReference toXContent(Exception error) throws IOException {
        try (XContentBuilder builder = XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE)) {
            builder.startObject();
            OpenSearchException.generateThrowableXContent(builder, ToXContent.EMPTY_PARAMS, error);
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }
}
