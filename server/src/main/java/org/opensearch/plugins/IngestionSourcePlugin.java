/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.index.IngestionShardConsumer;

import java.util.Collections;
import java.util.Map;

public interface IngestionSourcePlugin {

    interface IngestionSourceFactory<T extends IngestionShardConsumer<?>> {
        T create(IndexMetadata indexMetadata);
    }

    default Map<String, IngestionSourceFactory<? extends IngestionShardConsumer<?>>> getIngestionSourceFactories() {
        return Collections.emptyMap();
    }

}
