/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddomain;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Reads field-domain metadata from {@link IndexMetadata} custom data in cluster state.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class ClusterStateFieldDomainProvider implements FieldDomainProvider {
    private final IndexFieldDomainMetadata metadata;

    /**
     * Creates a provider using the built-in field-domain metadata parser.
     */
    public ClusterStateFieldDomainProvider() {
        this(IndexFieldDomainMetadata.getInstance());
    }

    ClusterStateFieldDomainProvider(IndexFieldDomainMetadata metadata) {
        this.metadata = Objects.requireNonNull(metadata, "metadata must not be null");
    }

    /**
     * Resolves domains from {@code index_field_domains} custom metadata for a concrete index.
     */
    @Override
    public Optional<FieldDomain> getDomain(ClusterState clusterState, String indexName, String field) {
        if (clusterState == null || indexName == null || field == null || field.isEmpty()) {
            return Optional.empty();
        }

        IndexMetadata indexMetadata = clusterState.metadata().index(indexName);
        if (indexMetadata == null) {
            return Optional.empty();
        }

        Map<String, String> custom = indexMetadata.getCustomData(IndexFieldDomainMetadata.CUSTOM_KEY);
        if (custom == null || custom.isEmpty()) {
            return Optional.empty();
        }

        return metadata.fromCustomData(custom, field);
    }
}
