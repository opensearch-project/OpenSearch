/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.Nullable;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * A request to make create point in time against one or more indices.
 */
public class CreatePitRequest extends ActionRequest implements IndicesRequest.Replaceable, ToXContent {

    // keep alive for pit reader context
    private TimeValue keepAlive;

    // this describes whether PIT can be created with partial failures
    private Boolean allowPartialPitCreation;
    @Nullable
    private String routing = null;
    @Nullable
    private String preference = null;
    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = SearchRequest.DEFAULT_INDICES_OPTIONS;

    public CreatePitRequest(TimeValue keepAlive, Boolean allowPartialPitCreation, String... indices) {
        this.keepAlive = keepAlive;
        this.allowPartialPitCreation = allowPartialPitCreation;
        this.indices = indices;
    }

    public CreatePitRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        routing = in.readOptionalString();
        preference = in.readOptionalString();
        keepAlive = in.readTimeValue();
        allowPartialPitCreation = in.readOptionalBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeOptionalString(routing);
        out.writeOptionalString(preference);
        out.writeTimeValue(keepAlive);
        out.writeOptionalBoolean(allowPartialPitCreation);
    }

    public String getRouting() {
        return routing;
    }

    public String getPreference() {
        return preference;
    }

    public String[] getIndices() {
        return indices;
    }

    public IndicesOptions getIndicesOptions() {
        return indicesOptions;
    }

    public TimeValue getKeepAlive() {
        return keepAlive;
    }

    /**
     * Sets if this request should allow partial results.
     */
    public void allowPartialPitCreation(Boolean allowPartialPitCreation) {
        this.allowPartialPitCreation = allowPartialPitCreation;
    }

    public boolean shouldAllowPartialPitCreation() {
        return allowPartialPitCreation;
    }

    public void setRouting(String routing) {
        this.routing = routing;
    }

    public void setPreference(String preference) {
        this.preference = preference;
    }

    public void setIndices(String[] indices) {
        this.indices = indices;
    }

    public void setIndicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = Objects.requireNonNull(indicesOptions, "indicesOptions must not be null");
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (keepAlive == null) {
            validationException = addValidationError("keep alive not specified", validationException);
        }
        return validationException;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public CreatePitRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = Objects.requireNonNull(indicesOptions, "indicesOptions must not be null");
        return this;
    }

    public void setKeepAlive(TimeValue keepAlive) {
        this.keepAlive = keepAlive;
    }

    public final String buildDescription() {
        StringBuilder sb = new StringBuilder();
        sb.append("indices[");
        Strings.arrayToDelimitedString(indices, ",", sb);
        sb.append("], ");
        sb.append("pointintime[").append(keepAlive).append("], ");
        sb.append("allowPartialPitCreation[").append(allowPartialPitCreation).append("], ");
        return sb.toString();
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new Task(id, type, action, this.buildDescription(), parentTaskId, headers);
    }

    private void validateIndices(String... indices) {
        Objects.requireNonNull(indices, "indices must not be null");
        for (String index : indices) {
            Objects.requireNonNull(index, "index must not be null");
        }
    }

    @Override
    public CreatePitRequest indices(String... indices) {
        validateIndices(indices);
        this.indices = indices;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("keep_alive", keepAlive);
        builder.field("allow_partial_pit_creation", allowPartialPitCreation);
        if (indices != null) {
            builder.startArray("indices");
            for (String index : indices) {
                builder.value(index);
            }
            builder.endArray();
        }
        if (indicesOptions != null) {
            indicesOptions.toXContent(builder, params);
        }
        return builder;
    }
}
