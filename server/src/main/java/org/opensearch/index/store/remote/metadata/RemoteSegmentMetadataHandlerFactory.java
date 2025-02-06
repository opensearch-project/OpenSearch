/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.metadata;

import org.opensearch.common.io.IndexIOStreamHandler;
import org.opensearch.common.io.IndexIOStreamHandlerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link RemoteSegmentMetadataHandlerFactory} is a factory class to create {@link RemoteSegmentMetadataHandler}
 * instances based on the {@link RemoteSegmentMetadata} version
 *
 * @opensearch.internal
 */
public class RemoteSegmentMetadataHandlerFactory implements IndexIOStreamHandlerFactory<RemoteSegmentMetadata> {

    private final ConcurrentHashMap<Integer, IndexIOStreamHandler<RemoteSegmentMetadata>> handlers = new ConcurrentHashMap<>();

    @Override
    public IndexIOStreamHandler<RemoteSegmentMetadata> getHandler(int version) {
        return handlers.computeIfAbsent(version, this::createHandler);
    }

    private IndexIOStreamHandler<RemoteSegmentMetadata> createHandler(int version) {
        return switch (version) {
            case RemoteSegmentMetadata.VERSION_ONE -> new RemoteSegmentMetadataHandler(RemoteSegmentMetadata.VERSION_ONE);
            case RemoteSegmentMetadata.VERSION_TWO -> new RemoteSegmentMetadataHandler(RemoteSegmentMetadata.VERSION_TWO);
            default -> throw new IllegalArgumentException("Unsupported RemoteSegmentMetadata version: " + version);
        };
    }
}
