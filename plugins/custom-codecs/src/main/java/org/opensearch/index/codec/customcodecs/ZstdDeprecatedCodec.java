/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Setting;
import org.opensearch.index.codec.CodecSettings;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.mapper.MapperService;

/**
 * ZstdDeprecatedCodec provides ZSTD compressor using the <a href="https://github.com/luben/zstd-jni">zstd-jni</a> library.
 * Added to support backward compatibility for indices created with Lucene95CustomCodec as codec name.
 */
@Deprecated(since = "2.10")
public class ZstdDeprecatedCodec extends Lucene95CustomCodec implements CodecSettings {

    /**
     * Creates a new ZstdDefaultCodec instance with the default compression level.
     */
    public ZstdDeprecatedCodec() {
        this(DEFAULT_COMPRESSION_LEVEL);
    }

    /**
     * Creates a new ZstdDefaultCodec instance.
     *
     * @param compressionLevel The compression level.
     */
    public ZstdDeprecatedCodec(int compressionLevel) {
        super(Mode.ZSTD_DEPRECATED, compressionLevel);
    }

    /**
     * Creates a new ZstdDefaultCodec instance.
     *
     * @param mapperService The mapper service.
     * @param logger The logger.
     * @param compressionLevel The compression level.
     */
    public ZstdDeprecatedCodec(MapperService mapperService, Logger logger, int compressionLevel) {
        super(Mode.ZSTD_DEPRECATED, compressionLevel, mapperService, logger);
    }

    /** The name for this codec. */
    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    @Override
    public boolean supports(Setting<?> setting) {
        return setting.equals(EngineConfig.INDEX_CODEC_COMPRESSION_LEVEL_SETTING);
    }
}
