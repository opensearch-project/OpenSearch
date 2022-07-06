/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodec;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.mapper.MapperService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

public class CustomCodecService extends CodecService {
    private final Map<String, Codec> codecs;

    public CustomCodecService(MapperService mapperService, Logger logger) {
        super(mapperService, logger);
        final MapBuilder<String, Codec> codecs = MapBuilder.<String, Codec>newMapBuilder();
        if (mapperService == null) {
            codecs.put(Lucene92CustomCodec.Mode.ZSTD.name(), new ZstdCodec());
            codecs.put(Lucene92CustomCodec.Mode.ZSTDNODICT.name(), new ZstdNoDictCodec());
            codecs.put(Lucene92CustomCodec.Mode.LZ4.name(), new Lz4Codec());
        } else {
            codecs.put(
                Lucene92CustomCodec.Mode.ZSTD.name(),
                new PerFieldMappingPostingFormatCodec(Lucene92CustomCodec.Mode.ZSTD, mapperService)
            );
            codecs.put(
                Lucene92CustomCodec.Mode.ZSTDNODICT.name(),
                new PerFieldMappingPostingFormatCodec(Lucene92CustomCodec.Mode.ZSTDNODICT, mapperService)
            );
            codecs.put(
                Lucene92CustomCodec.Mode.LZ4.name(),
                new PerFieldMappingPostingFormatCodec(Lucene92CustomCodec.Mode.LZ4, mapperService)
            );
        }
        this.codecs = codecs.immutableMap();
    }

    @Override
    public Codec codec(String name) {
        Codec codec = super.codec(name);
        if (codec == null) {
            codec = codecs.get(name);
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
