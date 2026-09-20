/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file to be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexAbstraction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.transport.client.IndicesAdminClient;

import java.util.Map;
import java.util.Objects;

/**
 * Applies compatible, additive mapping updates declared by a {@link SystemIndexDescriptor}.
 *
 * This utility does not create indices or transform existing documents. Changes that cannot be applied with a put-mapping request require
 * a separate reindex migration.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class SystemIndexMappingUpdater {
    /** Mapping metadata field containing the plugin-managed schema version. */
    public static final String SCHEMA_VERSION_META_FIELD = "schema_version";

    /** Version returned when an installed mapping has no schema version metadata. */
    public static final long NO_SCHEMA_VERSION = -1L;

    private static final String META_FIELD = "_meta";

    private SystemIndexMappingUpdater() {}

    /**
     * Updates the descriptor's concrete mapping target when its installed schema version is older than the desired version.
     *
     * A missing concrete index or write alias is reported as an unacknowledged response so callers can preserve ownership of index creation.
     * The listener is completed with an acknowledged response without issuing a request when the mapping is already current or newer.
     */
    public static void updateMappingIfNecessary(
        SystemIndexDescriptor descriptor,
        ClusterState clusterState,
        IndicesAdminClient client,
        ActionListener<AcknowledgedResponse> listener
    ) {
        Objects.requireNonNull(descriptor, "system index descriptor must not be null");
        Objects.requireNonNull(clusterState, "cluster state must not be null");
        Objects.requireNonNull(client, "indices admin client must not be null");
        Objects.requireNonNull(listener, "action listener must not be null");

        if (descriptor.hasMappings() == false) {
            listener.onFailure(new IllegalArgumentException("system index descriptor does not supply mappings"));
            return;
        }

        final IndexMetadata indexMetadata;
        try {
            indexMetadata = resolveMappingTarget(descriptor, clusterState);
            if (indexMetadata == null) {
                listener.onResponse(new AcknowledgedResponse(false));
                return;
            }

            if (getMappingVersion(indexMetadata) >= descriptor.getMappingVersion()) {
                listener.onResponse(new AcknowledgedResponse(true));
                return;
            }
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        PutMappingRequest request = new PutMappingRequest(indexMetadata.getIndex().getName()).source(
            descriptor.getMappings(),
            XContentType.JSON
        );
        client.putMapping(request, listener);
    }

    /**
     * Reads {@code _meta.schema_version} from installed index mappings.
     *
     * @return The installed version, or {@link #NO_SCHEMA_VERSION} when the index has no mappings or version metadata.
     */
    public static long getMappingVersion(IndexMetadata indexMetadata) {
        Objects.requireNonNull(indexMetadata, "index metadata must not be null");
        MappingMetadata mappingMetadata = indexMetadata.mapping();
        if (mappingMetadata == null) {
            return NO_SCHEMA_VERSION;
        }
        return getMappingVersion(mappingMetadata.sourceAsMap(), false);
    }

    static long getMappingVersion(String mappings) {
        Objects.requireNonNull(mappings, "mappings must not be null");
        Map<String, Object> source = XContentHelper.convertToMap(new BytesArray(mappings), false, XContentType.JSON).v2();
        return getMappingVersion(source, true);
    }

    private static IndexMetadata resolveMappingTarget(SystemIndexDescriptor descriptor, ClusterState clusterState) {
        if (descriptor.getPrimaryIndex() != null) {
            return clusterState.metadata().index(descriptor.getPrimaryIndex());
        }

        String writeAlias = descriptor.getWriteAlias();
        IndexAbstraction abstraction = clusterState.metadata().getIndicesLookup().get(writeAlias);
        if (abstraction == null) {
            return null;
        }
        if (abstraction.getType() != IndexAbstraction.Type.ALIAS) {
            throw new IllegalArgumentException("system index mapping target [" + writeAlias + "] is not an alias");
        }
        if (abstraction.getWriteIndex() == null) {
            throw new IllegalStateException("system index mapping alias [" + writeAlias + "] has no write index");
        }
        return abstraction.getWriteIndex();
    }

    private static long getMappingVersion(Map<String, Object> source, boolean required) {
        Object metadata = source.get(META_FIELD);
        if (metadata == null && source.size() == 1 && source.values().iterator().next() instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> nestedSource = (Map<String, Object>) source.values().iterator().next();
            metadata = nestedSource.get(META_FIELD);
        }

        if (metadata == null) {
            if (required) {
                throw new IllegalArgumentException("system index mappings must contain [_meta." + SCHEMA_VERSION_META_FIELD + "]");
            }
            return NO_SCHEMA_VERSION;
        }
        if ((metadata instanceof Map) == false) {
            throw new IllegalArgumentException("system index mapping [_meta] must be an object");
        }

        Object version = ((Map<?, ?>) metadata).get(SCHEMA_VERSION_META_FIELD);
        if (version == null) {
            if (required) {
                throw new IllegalArgumentException("system index mappings must contain [_meta." + SCHEMA_VERSION_META_FIELD + "]");
            }
            return NO_SCHEMA_VERSION;
        }
        if ((version instanceof Number) == false) {
            throw new IllegalArgumentException("system index mapping [_meta." + SCHEMA_VERSION_META_FIELD + "] must be a number");
        }

        Number numericVersion = (Number) version;
        long longVersion = numericVersion.longValue();
        if (longVersion < 0 || numericVersion.doubleValue() != longVersion) {
            throw new IllegalArgumentException(
                "system index mapping [_meta." + SCHEMA_VERSION_META_FIELD + "] must be a non-negative integer"
            );
        }
        return longVersion;
    }
}
