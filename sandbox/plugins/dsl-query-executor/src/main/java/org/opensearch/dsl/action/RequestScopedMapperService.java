/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.mapper.MapperService;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * Request-scoped, lazily created {@link MapperService} for the target index.
 *
 * <p>{@code IndicesService#createIndexMapperService} returns an <em>empty</em> MapperService:
 * the index mapping must be merged in before {@code fieldType()} lookups resolve anything.
 * This holder performs that merge ({@link MapperService.MergeReason#MAPPING_RECOVERY}, the
 * same reason the server's {@code MetadataMappingService} uses for this create-and-populate
 * pattern), caches the result so response building resolves at most one MapperService per
 * request, and releases the service's analyzer resources on {@link #close()} once the request
 * completes.
 *
 * <p>{@link #get()} returns null — degrading response typing to the translators' key-sampling
 * fallback instead of failing a query whose results already returned — when the index has been
 * deleted from cluster state mid-request, or when creating/merging the MapperService fails.
 */
final class RequestScopedMapperService implements Supplier<MapperService>, Closeable {

    private static final Logger logger = LogManager.getLogger(RequestScopedMapperService.class);

    private final String indexName;
    private final Supplier<ClusterState> clusterStateSupplier;
    private final CheckedFunction<IndexMetadata, MapperService, IOException> mapperServiceFactory;

    private MapperService mapperService;
    private boolean resolved;
    private boolean closed;

    /**
     * @param indexName the concrete index whose mapping backs response typing
     * @param clusterStateSupplier source of the current cluster state
     * @param mapperServiceFactory creates the standalone MapperService, typically
     *        {@code indicesService::createIndexMapperService}
     */
    RequestScopedMapperService(
        String indexName,
        Supplier<ClusterState> clusterStateSupplier,
        CheckedFunction<IndexMetadata, MapperService, IOException> mapperServiceFactory
    ) {
        this.indexName = indexName;
        this.clusterStateSupplier = clusterStateSupplier;
        this.mapperServiceFactory = mapperServiceFactory;
    }

    /**
     * Returns the merged MapperService for the target index, creating it on first call, or
     * null when the index or its mapping cannot be resolved (see class javadoc).
     */
    @Override
    public synchronized MapperService get() {
        if (closed) {
            return null;
        }
        if (resolved == false) {
            resolved = true;
            mapperService = resolve();
        }
        return mapperService;
    }

    private MapperService resolve() {
        try {
            IndexMetadata indexMetadata = clusterStateSupplier.get().metadata().index(indexName);
            if (indexMetadata == null) {
                logger.warn("index [{}] not found in cluster state; response typing degrades to key sampling", indexName);
                return null;
            }
            MapperService created = mapperServiceFactory.apply(indexMetadata);
            try {
                // The freshly created MapperService is empty — merge the index mapping so
                // fieldType() resolves. Without this, every lookup returns null and terms
                // responses silently fall back to sampled typing (RAW-formatted keys).
                created.merge(indexMetadata, MapperService.MergeReason.MAPPING_RECOVERY);
                return created;
            } catch (Exception e) {
                IOUtils.closeWhileHandlingException(created);
                throw e;
            }
        } catch (Exception e) {
            logger.warn("Failed to resolve MapperService for index [{}]; response typing degrades to key sampling", indexName, e);
            return null;
        }
    }

    /** Releases the MapperService's analyzer resources. Safe to call more than once. */
    @Override
    public synchronized void close() {
        closed = true;
        if (mapperService != null) {
            IOUtils.closeWhileHandlingException(mapperService);
            mapperService = null;
        }
    }
}
