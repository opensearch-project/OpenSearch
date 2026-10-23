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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.mapper.MapperService;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Request-scoped, lazily created {@link MapperService} for response key typing, pinned to the
 * index mapping captured when the request started. {@code IndicesService#createIndexMapperService}
 * returns an <em>empty</em> MapperService, so the holder merges the pinned mapping in before
 * handing it out ({@link MapperService.MergeReason#MAPPING_RECOVERY}, the same create-and-merge
 * pattern the server's {@code MetadataMappingService} uses), memoizes the result, and releases
 * it on {@link #close()}. {@link #get()} returns null only when creating or merging fails.
 */
final class RequestScopedMapperService implements Supplier<MapperService>, Closeable {

    private static final Logger logger = LogManager.getLogger(RequestScopedMapperService.class);

    private final IndexMetadata indexMetadata;
    private final CheckedFunction<IndexMetadata, MapperService, IOException> mapperServiceFactory;

    private MapperService mapperService;
    private boolean resolved;
    private boolean closed;

    RequestScopedMapperService(
        IndexMetadata indexMetadata,
        CheckedFunction<IndexMetadata, MapperService, IOException> mapperServiceFactory
    ) {
        this.indexMetadata = Objects.requireNonNull(indexMetadata, "indexMetadata must not be null");
        this.mapperServiceFactory = mapperServiceFactory;
    }

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
            MapperService created = mapperServiceFactory.apply(indexMetadata);
            try {
                created.merge(indexMetadata, MapperService.MergeReason.MAPPING_RECOVERY);
                return created;
            } catch (Exception e) {
                IOUtils.closeWhileHandlingException(created);
                throw e;
            }
        } catch (Exception e) {
            logger.warn("Failed to resolve MapperService for index [{}]", indexMetadata.getIndex().getName(), e);
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
