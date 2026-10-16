/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.arrow;

import java.io.IOException;
import java.util.Map;
import org.opensearch.cluster.metadata.IngestionSource;

/**
 * Extension point for organization-specific Arrow data sources.
 *
 * <p>To support a new dataset type without forking this plugin, implement this interface (and
 * {@link ArrowSource}) in a separate plugin that:
 *
 * <ol>
 *   <li>declares {@code extendedPlugins = ['ingestion-arrow']} in its {@code build.gradle}, and
 *   <li>registers the implementation via the standard Java {@code ServiceLoader} mechanism, i.e. a
 *       {@code META-INF/services/org.opensearch.plugin.arrow.ArrowSourceFactory} resource file
 *       listing the implementation's fully-qualified class name.
 * </ol>
 *
 * <p>{@link ArrowPlugin} discovers every {@link ArrowSourceFactory} contributed this way at node
 * startup (see {@link ArrowPlugin#loadExtensions}) and dispatches to the one matching {@link
 * ArrowIngestionConfig#getSourceType()}. This lets organization-specific concerns — credential
 * management, proprietary storage systems, custom authentication flows, or internal service
 * discovery — stay entirely outside of this plugin.
 */
public interface ArrowSourceFactory {

    /**
     * Returns the dataset type this factory handles.
     *
     * @return the dataset type this factory handles, e.g. {@code "LANCE"}. Must be unique across
     *     every {@link ArrowSourceFactory} installed on the node; node startup fails if two
     *     factories register the same type.
     */
    String getType();

    /**
     * Creates an {@link ArrowSource} for one shard consumer.
     *
     * @param params the raw {@link IngestionSource#params()} map, so implementations can read
     *     their own source-specific keys in addition to the common ones already parsed into {@code
     *     ingestionConfig}
     * @param ingestionConfig the common, dataset-agnostic ingestion config parsed by this plugin
     */
    ArrowSource createArrowSource(Map<String, Object> params, ArrowIngestionConfig ingestionConfig)
            throws IOException;
}
