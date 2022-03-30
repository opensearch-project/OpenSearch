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
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.action.ValidateActions.addValidationError;

public class CreatePITRequest extends ActionRequest implements IndicesRequest.Replaceable {

    private TimeValue keepAlive;
    private final boolean allowPartialPitCreation;
    @Nullable
    private String routing = null;
    @Nullable
    private String preference = null;
    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = SearchRequest.DEFAULT_INDICES_OPTIONS;

    public CreatePITRequest(TimeValue keepAlive, boolean allowPartialPitCreation) {
        this.keepAlive = keepAlive;
        this.allowPartialPitCreation = allowPartialPitCreation;
    }

    public CreatePITRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        routing = in.readOptionalString();
        preference = in.readOptionalString();
        keepAlive = in.readTimeValue();
        routing = in.readOptionalString();
        allowPartialPitCreation = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeOptionalString(preference);
        out.writeTimeValue(keepAlive);
        out.writeOptionalString(routing);
        out.writeBoolean(allowPartialPitCreation);
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

    public boolean isAllowPartialPitCreation() {
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
            validationException = addValidationError("Keep alive is missing", validationException);
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

    public void setKeepAlive(TimeValue keepAlive) {
        this.keepAlive = keepAlive;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new SearchTask(id, type, action, () -> "desc", parentTaskId, headers);
    }

    /**
     * Sets the indices the search will be executed on.
     */
    @Override
    public CreatePITRequest indices(String... indices) {
        SearchRequest.validateIndices(indices);
        this.indices = indices;
        return this;
    }
}
