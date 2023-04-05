/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.mapper.MapperService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

/**
 * CustomCodecService provides ZSTD and ZSTDNODICT compression codecs.
 */
public class CustomCodecService extends CodecService {
    private final Map<String, Codec> codecs;

    /**
     * Creates a new CustomCodecService.
     *
     * @param mapperService A mapper service.
     * @param logger A logger.
     */
    public CustomCodecService(MapperService mapperService, Logger logger) {
        super(mapperService, logger);
        final MapBuilder<String, Codec> codecs = MapBuilder.<String, Codec>newMapBuilder();
        if (mapperService == null) {
            codecs.put(Lucene95CustomCodec.Mode.ZSTD.name(), new ZstdCodec());
            codecs.put(Lucene95CustomCodec.Mode.ZSTDNODICT.name(), new ZstdNoDictCodec());
        } else {
            codecs.put(
                Lucene95CustomCodec.Mode.ZSTD.name(),
                new PerFieldMappingPostingFormatCodec(Lucene95CustomCodec.Mode.ZSTD, mapperService)
            );
            codecs.put(
                Lucene95CustomCodec.Mode.ZSTDNODICT.name(),
                new PerFieldMappingPostingFormatCodec(Lucene95CustomCodec.Mode.ZSTDNODICT, mapperService)
            );
        }
        this.codecs = codecs.immutableMap();
    }

    @Override
    public Codec codec(String name) {
        Codec codec = codecs.get(name);
        if (codec == null) {
            return super.codec(name);
        }
        return codec;
    }

    @Override
    public String[] availableCodecs() {
        ArrayList<String> ac = new ArrayList<String>(Arrays.asList(super.availableCodecs()));
        ac.addAll(codecs.keySet());
        return ac.toArray(new String[0]);
    }
}
