/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering.status.model;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Migration status request for a single index.
 */
public class GetTieringStatusRequest extends ClusterManagerNodeReadRequest<GetTieringStatusRequest> {

    private String index;
    private Boolean isDetailedFlagEnabled;

    /** Returns the detailed flag. */
    public Boolean getDetailedFlag() {
        return isDetailedFlagEnabled;
    }

    /**
     * Sets the detailed flag.
     * @param detailedFlagEnabled the flag value
     */
    public void setDetailedFlagEnabled(Boolean detailedFlagEnabled) {
        isDetailedFlagEnabled = detailedFlagEnabled;
    }

    /** Constructs a default request. */
    public GetTieringStatusRequest() {}

    /**
     * Constructs a request for the given index.
     * @param index the index name
     */
    public GetTieringStatusRequest(String index) {
        this.index = index;
        this.isDetailedFlagEnabled = false;
    }

    /**
     * Constructs a request for the given index with detailed flag.
     * @param index the index name
     * @param isDetailedFlagEnabled whether detailed info is enabled
     */
    public GetTieringStatusRequest(String index, Boolean isDetailedFlagEnabled) {
        this.index = index;
        this.isDetailedFlagEnabled = isDetailedFlagEnabled;
    }

    /**
     * Constructs a request from a stream.
     * @param in the stream input
     * @throws IOException if an I/O error occurs
     */
    public GetTieringStatusRequest(StreamInput in) throws IOException {
        super(in);
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public ActionRequestValidationException validate() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Sets the index name.
     * @param index the index name
     */
    public void setIndex(String index) {
        this.index = index;
    }

    /** Returns the index name. */
    public String getIndex() {
        return index;
    }
}
