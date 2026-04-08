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

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;
import static org.opensearch.action.search.SearchRequest.DEFAULT_INDICES_OPTIONS;

/**
 * Request to get a single saved object by index and document id.
 */
public class GetSavedObjectRequest extends ActionRequest implements IndicesRequest.Replaceable {

    private String index;
    private String documentId;

    public GetSavedObjectRequest() {}

    public GetSavedObjectRequest(String index, String documentId) {
        this.index = index;
        this.documentId = documentId;
    }

    public GetSavedObjectRequest(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
        this.documentId = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeString(documentId);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (index == null || index.isEmpty()) {
            validationException = addValidationError("index is required", validationException);
        }
        if (documentId == null || documentId.isEmpty()) {
            validationException = addValidationError("documentId is required", validationException);
        }
        return validationException;
    }

    public String getIndex() {
        return index;
    }

    public String getDocumentId() {
        return documentId;
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
