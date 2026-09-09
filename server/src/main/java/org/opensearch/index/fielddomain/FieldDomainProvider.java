/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddomain;

import org.opensearch.cluster.ClusterState;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Optional;

/**
 * Resolves index-level field domains for a concrete index from a metadata source.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface FieldDomainProvider {

    /**
     * Returns field domains for the given concrete index and field.
     *
     * The {@code indexName} is resolved against the supplied {@link ClusterState}. Callers that need UUID-level identity
     * guarantees must ensure the name refers to the intended concrete index in that same cluster state.
     *
     * @param clusterState cluster state visible to the coordinator
     * @param indexName concrete index name
     * @param field field name
     * @return parsed domain when present and supported; otherwise empty
     */
    Optional<FieldDomain> getDomain(ClusterState clusterState, String indexName, String field);
}
