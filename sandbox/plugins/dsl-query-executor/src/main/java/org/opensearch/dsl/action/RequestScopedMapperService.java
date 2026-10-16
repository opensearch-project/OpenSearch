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
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Request-scoped, lazily created {@link MapperService}(s) for response key typing, pinned to the
 * index mappings captured when the request started. {@code IndicesService#createIndexMapperService}
 * returns an <em>empty</em> MapperService, so the holder merges the pinned mapping in before
 * handing it out ({@link MapperService.MergeReason#MAPPING_RECOVERY}, the same create-and-merge
 * pattern the server's {@code MetadataMappingService} uses), memoizes the result, and releases
 * it on {@link #close()}.
 *
 * <p>Holds the request's concrete indices in resolution order; each index's MapperService is built
 * lazily on first use and cached, so a single-index request builds exactly one and later indices
 * are built only when an earlier one does not answer. {@link #get()} exposes the first index's
 * service, preserving the historical single-index behaviour.
 */
final class RequestScopedMapperService implements Supplier<MapperService>, Closeable {

    private static final Logger logger = LogManager.getLogger(RequestScopedMapperService.class);

    private final List<IndexMetadata> indices;
    private final CheckedFunction<IndexMetadata, MapperService, IOException> mapperServiceFactory;

    private final MapperService[] mapperServices;
    private final boolean[] resolved;
    private boolean closed;

    /** Single-index convenience constructor, preserving the historical single-index contract. */
    RequestScopedMapperService(
        IndexMetadata indexMetadata,
        CheckedFunction<IndexMetadata, MapperService, IOException> mapperServiceFactory
    ) {
        this(List.of(Objects.requireNonNull(indexMetadata, "indexMetadata must not be null")), mapperServiceFactory);
    }

    RequestScopedMapperService(
        List<IndexMetadata> indices,
        CheckedFunction<IndexMetadata, MapperService, IOException> mapperServiceFactory
    ) {
        this.indices = List.copyOf(Objects.requireNonNull(indices, "indices must not be null"));
        if (this.indices.isEmpty()) {
            throw new IllegalArgumentException("indices must not be empty");
        }
        this.mapperServiceFactory = mapperServiceFactory;
        this.mapperServices = new MapperService[this.indices.size()];
        this.resolved = new boolean[this.indices.size()];
    }

    /**
     * Returns the first (on a single-index request, the only) index's MapperService, or null when
     * creating or merging it fails or the holder is closed.
     */
    @Override
    public synchronized MapperService get() {
        return mapperFor(0);
    }

    /**
     * Resolves a field's mapping by trying each index in resolution order and returning the first
     * non-null match, building an index's MapperService only when the lookup falls through to it.
     *
     * @param field the full (dotted) field name
     * @return the first defining index's mapping, or null when no index defines it
     */
    synchronized MappedFieldType fieldType(String field) {
        for (int i = 0; i < indices.size(); i++) {
            MapperService mapperService = mapperFor(i);
            if (mapperService == null) {
                continue;
            }
            MappedFieldType fieldType = mapperService.fieldType(field);
            if (fieldType != null) {
                return fieldType;
            }
        }
        return null;
    }

    /**
     * Resolves a field's mapping within a single one of the request's indices — the per-index view
     * the schema-equivalence gate compares across indices.
     *
     * @param index one of the request's resolved indices
     * @param field the full (dotted) field name
     * @return that index's mapping for the field, or null when the index is not part of this
     *         request or does not define the field
     */
    synchronized MappedFieldType fieldType(IndexMetadata index, String field) {
        for (int i = 0; i < indices.size(); i++) {
            IndexMetadata candidate = indices.get(i);
            if (candidate == index || candidate.getIndex().equals(index.getIndex())) {
                MapperService mapperService = mapperFor(i);
                return mapperService == null ? null : mapperService.fieldType(field);
            }
        }
        return null;
    }

    private MapperService mapperFor(int i) {
        if (closed || i >= indices.size()) {
            return null;
        }
        if (resolved[i] == false) {
            resolved[i] = true;
            mapperServices[i] = resolve(indices.get(i));
        }
        return mapperServices[i];
    }

    private MapperService resolve(IndexMetadata indexMetadata) {
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

    /** Releases every MapperService built during the request. Safe to call more than once. */
    @Override
    public synchronized void close() {
        closed = true;
        for (int i = 0; i < mapperServices.length; i++) {
            if (mapperServices[i] != null) {
                IOUtils.closeWhileHandlingException(mapperServices[i]);
                mapperServices[i] = null;
            }
        }
    }
}
