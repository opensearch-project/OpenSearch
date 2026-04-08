/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dashboards.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;
import static org.opensearch.action.search.SearchRequest.DEFAULT_INDICES_OPTIONS;

/**
 * Request to search saved objects within a dashboards index.
 */
public class SearchSavedObjectRequest extends ActionRequest implements IndicesRequest.Replaceable {

    private String index;
    private SearchSourceBuilder source;

    public SearchSavedObjectRequest() {}

    public SearchSavedObjectRequest(String index, SearchSourceBuilder source) {
        this.index = index;
        this.source = source;
    }

    public SearchSavedObjectRequest(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
        this.source = in.readOptionalWriteable(SearchSourceBuilder::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeOptionalWriteable(source);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (index == null || index.isEmpty()) {
            validationException = addValidationError("index is required", validationException);
        }
        return validationException;
    }

    public String getIndex() {
        return index;
    }

    public SearchSourceBuilder getSource() {
        return source;
    }

    @Override
    public IndicesRequest indices(String... indices) {
        this.index = indices[0];
        return this;
    }

    @Override
    public String[] indices() {
        return new String[] { this.index };
    }

    @Override
    public IndicesOptions indicesOptions() {
        return DEFAULT_INDICES_OPTIONS;
    }
}
