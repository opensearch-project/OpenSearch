/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.tasks;

import org.opensearch.OpenSearchException;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.InstantiatingObjectParser;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ObjectParserHelper;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentHelper;
import org.opensearch.transport.client.Requests;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.opensearch.common.xcontent.XContentHelper.convertToMap;
import static org.opensearch.common.xcontent.XContentHelper.writeRawField;
import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Information about a running task or a task that stored its result. Running tasks just have a {@link #getTask()} while
 * tasks with stored result will have either a {@link #getError()} or {@link #getResponse()}.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class TaskResult implements Writeable, ToXContentObject {
    private final boolean completed;
    private final TaskInfo task;
    @Nullable
    private final BytesReference error;
    @Nullable
    private final BytesReference response;

    /**
     * Construct a {@linkplain TaskResult} for a task for which we don't have a result or error. That usually means that the task
     * is incomplete, but it could also mean that we waited for the task to complete but it didn't save any error information.
     */
    public TaskResult(boolean completed, TaskInfo task) {
        this(completed, task, null, null);
    }

    /**
     * Construct a {@linkplain TaskResult} for a task that completed with an error.
     */
    public TaskResult(TaskInfo task, Exception error) throws IOException {
        this(true, task, toXContent(error), null);
    }

    /**
     * Construct a {@linkplain TaskResult} for a task that completed successfully.
     */
    public TaskResult(TaskInfo task, ToXContent response) throws IOException {
        this(true, task, null, XContentHelper.toXContent(response, Requests.INDEX_CONTENT_TYPE, true));
    }

    public TaskResult(boolean completed, TaskInfo task, @Nullable BytesReference error, @Nullable BytesReference result) {
        this.completed = completed;
        this.task = requireNonNull(task, "task is required");
        this.error = error;
        this.response = result;
    }

    /**
     * Read from a stream.
     */
    public TaskResult(StreamInput in) throws IOException {
        completed = in.readBoolean();
        task = new TaskInfo(in);
        error = in.readOptionalBytesReference();
        response = in.readOptionalBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(completed);
        task.writeTo(out);
        out.writeOptionalBytesReference(error);
        out.writeOptionalBytesReference(response);
    }

    /**
     * Get the task that this wraps.
     */
    public TaskInfo getTask() {
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
            writeRawField("error", error, builder, params);
        }
        if (response != null) {
            writeRawField("response", response, builder, params);
        }
        return builder;
    }

    public static final InstantiatingObjectParser<TaskResult, Void> PARSER;

    static {
        InstantiatingObjectParser.Builder<TaskResult, Void> parser = InstantiatingObjectParser.builder(
            "stored_task_result",
            true,
            TaskResult.class
        );
        parser.declareBoolean(constructorArg(), new ParseField("completed"));
        parser.declareObject(constructorArg(), TaskInfo.PARSER, new ParseField("task"));
        ObjectParserHelper<TaskResult, Void> parserHelper = new ObjectParserHelper<>();
        parserHelper.declareRawObject(parser, optionalConstructorArg(), new ParseField("error"));
        parserHelper.declareRawObject(parser, optionalConstructorArg(), new ParseField("response"));
        PARSER = parser.build();
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

    // Implements equals and hashcode for testing
    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != TaskResult.class) {
            return false;
        }
        TaskResult other = (TaskResult) obj;
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
        try (XContentBuilder builder = MediaTypeRegistry.contentBuilder(Requests.INDEX_CONTENT_TYPE)) {
            builder.startObject();
            OpenSearchException.generateThrowableXContent(builder, ToXContent.EMPTY_PARAMS, error);
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }
}
