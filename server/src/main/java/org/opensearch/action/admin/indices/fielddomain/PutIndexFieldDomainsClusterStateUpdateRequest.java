/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.fielddomain;

import org.opensearch.cluster.ack.ClusterStateUpdateRequest;
import org.opensearch.core.index.Index;

import java.util.Map;
import java.util.Objects;

/**
 * Cluster state update request for inserting field-domain metadata into one index.
 *
 * The transport layer resolves action-specific request details and hands this smaller request to the metadata service,
 * following the same split used by other index metadata update actions.
 */
public class PutIndexFieldDomainsClusterStateUpdateRequest extends ClusterStateUpdateRequest<
    PutIndexFieldDomainsClusterStateUpdateRequest> {
    private Index targetIndex;
    private Map<String, String> fieldDomainCustomData = Map.of();

    /**
     * Returns the exact target concrete index.
     */
    public Index targetIndex() {
        return targetIndex;
    }

    /**
     * Sets the exact target concrete index.
     */
    public PutIndexFieldDomainsClusterStateUpdateRequest targetIndex(Index targetIndex) {
        this.targetIndex = targetIndex;
        return this;
    }

    /**
     * Returns encoded field-domain metadata.
     */
    public Map<String, String> fieldDomainCustomData() {
        return fieldDomainCustomData;
    }

    /**
     * Sets encoded field-domain metadata.
     */
    public PutIndexFieldDomainsClusterStateUpdateRequest fieldDomainCustomData(Map<String, String> fieldDomainCustomData) {
        this.fieldDomainCustomData = Map.copyOf(Objects.requireNonNull(fieldDomainCustomData, "fieldDomainCustomData must not be null"));
        return this;
    }
}
