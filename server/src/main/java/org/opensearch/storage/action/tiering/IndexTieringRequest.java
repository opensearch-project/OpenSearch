/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.AcknowledgedRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.storage.common.tiering.TieringUtils.Tier;

import java.io.IOException;
import java.util.Objects;

import static org.opensearch.action.ValidateActions.addValidationError;
import static org.opensearch.storage.common.tiering.TieringUtils.isMigrationAllowed;

/**
 * Represents a request to move indices between different storage tiers.
 * This class handles the validation and serialization of tiering requests.
 */
public class IndexTieringRequest extends AcknowledgedRequest<IndexTieringRequest> {

    private final String indexName;
    private final Tier targetTier;

    /**
     * Constructs a new tiering request.
     *
     * @param targetTier the target tier for the index
     * @param indexName the index name to be tiered
     */
    public IndexTieringRequest(final String targetTier, final String indexName) {
        this.targetTier = Tier.fromString(targetTier);
        this.indexName = indexName;
    }

    /**
     * Constructs a new tiering request from a stream.
     * @param in the stream input
     * @throws IOException if an I/O error occurs
     */
    public IndexTieringRequest(StreamInput in) throws IOException {
        super(in);
        indexName = in.readString();
        targetTier = Tier.fromString(in.readString());
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indexName == null) {
            validationException = addValidationError("Index parameter cannot be null", validationException);
        } else if (indexName.isBlank()) {
            validationException = addValidationError("Index parameter cannot be empty", validationException);
        } else if (isMigrationAllowed(indexName) == false) {
            validationException = addValidationError("Index '" + indexName + "' is blocklisted for migrations", validationException);
        }
        if (targetTier == null) {
            validationException = addValidationError("Target tier cannot be null", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(indexName);
        out.writeString(targetTier.value());
    }

    /**
     * Returns the target tier for this request.
     *
     * @return the target Tier enum value
     */
    public Tier tier() {
        return targetTier;
    }

    /**
     * Returns the index name for this tiering request.
     *
     * @return the index name
     */
    public String getIndex() {
        return indexName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexTieringRequest that = (IndexTieringRequest) o;
        return clusterManagerNodeTimeout.equals(that.clusterManagerNodeTimeout)
            && timeout.equals(that.timeout)
            && indexName.equals(that.indexName)
            && targetTier.equals(that.targetTier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterManagerNodeTimeout, timeout, indexName, targetTier);
    }
}
