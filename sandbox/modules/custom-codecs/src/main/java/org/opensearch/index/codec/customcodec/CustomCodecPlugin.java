/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodec;

import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.index.codec.CodecServiceFactory;
import org.opensearch.index.IndexSettings;

import java.util.Optional;

/**
 * A plugin that implements custom codecs. Supports these codecs:
 * <ul>
 * <li>ZSTD
 * <li>ZSTDNODICT
 * </ul>
 *
 * @opensearch.internal
 */
public final class CustomCodecPlugin extends Plugin implements EnginePlugin {

    /** Creates a new instance */
    public CustomCodecPlugin() {}

    /**
     * @param indexSettings is the default indexSettings
     * @return the engine factory
     */
    @Override
    public Optional<CodecServiceFactory> getCustomCodecServiceFactory(final IndexSettings indexSettings) {
        return Optional.of(new CustomCodecServiceFactory());
    }
}
