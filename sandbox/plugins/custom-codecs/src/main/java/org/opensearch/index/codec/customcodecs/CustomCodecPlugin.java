/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.index.codec.CodecServiceFactory;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineConfig;

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

    /** ZSTD compression with dictionary */
    protected static final String ZSTD_CODEC_NAME = "ZSTD";

    /** ZSTD compression without dictionary */
    protected static final String ZSTDNODICT_CODEC_NAME = "ZSTDNODICT";

    /** Creates a new instance */
    public CustomCodecPlugin() {}

    /**
     * @param indexSettings is the default indexSettings
     * @return the custom-codec service factory
     */
    @Override
    public Optional<CodecServiceFactory> getCustomCodecServiceFactory(final IndexSettings indexSettings) {
        String codec = indexSettings.getValue(EngineConfig.INDEX_CODEC_SETTING);
        if (codec.equals(ZSTD_CODEC_NAME) || codec.equals(ZSTDNODICT_CODEC_NAME)) {
            return Optional.of(new CustomCodecServiceFactory());
        }
        return Optional.empty();
    }
}
