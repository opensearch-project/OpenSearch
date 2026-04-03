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
import org.opensearch.storage.common.tiering.TieringUtils.Tier;

import java.io.IOException;

/**
 * Represents a request to move indices between different storage tiers.
 * This class handles the validation and serialization of tiering requests.
 *
 * Serialization (writeTo, StreamInput constructor), validation, equals/hashCode
 * will be added in the implementation PR.
 */
public class IndexTieringRequest extends AcknowledgedRequest<IndexTieringRequest> {

    private final String indexName;
    private final Tier targetTier;

    /**
     * Constructs a new tiering request from a stream.
     * @param in the stream input
     * @throws IOException if an I/O error occurs
     */
    public IndexTieringRequest(StreamInput in) throws IOException {
        super(in);
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Constructs a new tiering request.
     * @param targetTier the target tier
     * @param indexName the index name
     */
    public IndexTieringRequest(final String targetTier, final String indexName) {
        this.targetTier = Tier.fromString(targetTier);
        this.indexName = indexName;
    }

    @Override
    public ActionRequestValidationException validate() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /** Returns the target tier. */
    public Tier tier() {
        return targetTier;
    }

    /** Returns the index name. */
    public String getIndex() {
        return indexName;
    }
}
