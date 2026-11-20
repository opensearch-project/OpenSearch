/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.termsenum;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * A request to list the distinct terms of one field, optionally restricted to a prefix, across the target indices.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class TermsEnumRequest extends ActionRequest implements IndicesRequest.Replaceable {

    /** The number of terms returned when {@code size} is not set. */
    public static final int DEFAULT_SIZE = 10;
    /** The longest accepted {@code string} prefix, in characters. */
    public static final int MAX_STRING_LENGTH = 1000;
    /** The time budget used when {@code timeout} is not set. */
    public static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueSeconds(1);

    private String[] indices;
    // as in search (without the legacy ignore_throttled): a closed target fails with index_closed_exception, or is skipped
    // with ignore_unavailable
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpenAndForbidClosed();
    private String field;
    private String string = "";
    private int size = DEFAULT_SIZE;
    private boolean caseInsensitive = false;
    private TimeValue timeout = DEFAULT_TIMEOUT;
    private QueryBuilder indexFilter;
    private String searchAfter;

    /**
     * Creates a request against the given indices.
     *
     * @param indices the indices, data streams, aliases or patterns to enumerate
     */
    public TermsEnumRequest(String... indices) {
        this.indices = indices;
    }

    /**
     * Reads a request from the wire.
     *
     * @param in the stream to read from
     * @throws IOException if reading fails
     */
    public TermsEnumRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        field = in.readString();
        string = in.readString();
        size = in.readVInt();
        caseInsensitive = in.readBoolean();
        timeout = in.readTimeValue();
        indexFilter = in.readOptionalNamedWriteable(QueryBuilder.class);
        searchAfter = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeString(field);
        out.writeString(string);
        out.writeVInt(size);
        out.writeBoolean(caseInsensitive);
        out.writeTimeValue(timeout);
        out.writeOptionalNamedWriteable(indexFilter);
        out.writeOptionalString(searchAfter);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException e = null;
        if (field == null) {
            e = addValidationError("[field] is required", e);
        }
        if (size < 1) {
            e = addValidationError("[size] must be at least 1 but was [" + size + "]", e);
        }
        if (string.length() > MAX_STRING_LENGTH) {
            e = addValidationError("[string] must be at most [" + MAX_STRING_LENGTH + "] characters but was [" + string.length() + "]", e);
        }
        // the coordinator counts whole milliseconds, so a positive timeout below 1ms is rejected rather than truncated to 0
        if (timeout.millis() < 1) {
            e = addValidationError("[timeout] must be at least 1ms but was [" + timeout + "]", e);
        }
        return e;
    }

    /** @return the field whose terms are listed */
    public String field() {
        return field;
    }

    /**
     * Sets the field whose terms are listed.
     *
     * @param field the field name
     * @return this request
     */
    public TermsEnumRequest field(String field) {
        this.field = field;
        return this;
    }

    /** @return the prefix every returned term must start with, {@code ""} for none */
    public String string() {
        return string;
    }

    /**
     * Sets the prefix every returned term must start with.
     *
     * @param string the prefix, where {@code null} means the empty prefix
     * @return this request
     */
    public TermsEnumRequest string(String string) {
        this.string = string == null ? "" : string;
        return this;
    }

    /** @return the maximum number of terms returned */
    public int size() {
        return size;
    }

    /**
     * Sets the maximum number of terms returned.
     *
     * @param size the maximum number of terms
     * @return this request
     */
    public TermsEnumRequest size(int size) {
        this.size = size;
        return this;
    }

    /** @return whether the prefix match ignores ASCII case */
    public boolean caseInsensitive() {
        return caseInsensitive;
    }

    /**
     * Sets whether the prefix match ignores ASCII case.
     *
     * @param caseInsensitive true to fold ASCII case
     * @return this request
     */
    public TermsEnumRequest caseInsensitive(boolean caseInsensitive) {
        this.caseInsensitive = caseInsensitive;
        return this;
    }

    /** @return the time budget of the whole request */
    public TimeValue timeout() {
        return timeout;
    }

    /**
     * Sets the time budget of the whole request.
     *
     * @param timeout the budget, which must be at least 1ms
     * @return this request
     */
    public TermsEnumRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    /** @return the query used to skip shards that cannot match, or {@code null} */
    public QueryBuilder indexFilter() {
        return indexFilter;
    }

    /**
     * Sets a query used to skip shards for which it cannot match.
     *
     * @param indexFilter the filter, or {@code null} for none
     * @return this request
     */
    public TermsEnumRequest indexFilter(QueryBuilder indexFilter) {
        this.indexFilter = indexFilter;
        return this;
    }

    /** @return the term after which (in byte order) results start, or {@code null} */
    public String searchAfter() {
        return searchAfter;
    }

    /**
     * Sets the term after which, in UTF-8 byte order, results start.
     *
     * @param searchAfter the term, or {@code null} for none
     * @return this request
     */
    public TermsEnumRequest searchAfter(String searchAfter) {
        this.searchAfter = searchAfter;
        return this;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public TermsEnumRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    /**
     * Sets how unavailable and wildcard targets are handled.
     *
     * @param indicesOptions the options
     * @return this request
     */
    public TermsEnumRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers) {
            @Override
            public boolean shouldCancelChildrenOnCancellation() {
                return true;
            }
        };
    }

    @Override
    public String getDescription() {
        return "terms_enum indices[" + String.join(",", indices == null ? new String[0] : indices) + "], field[" + field + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TermsEnumRequest that = (TermsEnumRequest) o;
        return size == that.size
            && caseInsensitive == that.caseInsensitive
            && Arrays.equals(indices, that.indices)
            && Objects.equals(indicesOptions, that.indicesOptions)
            && Objects.equals(field, that.field)
            && Objects.equals(string, that.string)
            && Objects.equals(timeout, that.timeout)
            && Objects.equals(indexFilter, that.indexFilter)
            && Objects.equals(searchAfter, that.searchAfter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            Arrays.hashCode(indices),
            indicesOptions,
            field,
            string,
            size,
            caseInsensitive,
            timeout,
            indexFilter,
            searchAfter
        );
    }
}
