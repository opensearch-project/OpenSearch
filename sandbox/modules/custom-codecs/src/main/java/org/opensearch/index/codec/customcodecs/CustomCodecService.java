/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.apache.lucene.codecs.Codec;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.codec.CodecServiceConfig;
import org.opensearch.index.mapper.MapperService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

/**
 * CustomCodecService provides QDEFLATE and QLZ4 compression codecs.
 */
public class CustomCodecService extends CodecService {
    private final Map<String, Codec> codecs;

    /**
     * Parameterized ctor for CustomCodecService
     * @param codecServiceConfig Generic codec service config
     */
    public CustomCodecService(CodecServiceConfig codecServiceConfig) {
        super(codecServiceConfig.getMapperService(), codecServiceConfig.getIndexSettings(), codecServiceConfig.getLogger());
        MapperService mapperService = codecServiceConfig.getMapperService();

        final MapBuilder<String, Codec> codecs = MapBuilder.<String, Codec>newMapBuilder();
        String accelerationMode = codecServiceConfig.getIndexSettings().getValue(Lucene99QatCodec.INDEX_CODEC_MODE_SETTING);
        if (mapperService == null) {
            codecs.put(Lucene99QatCodec.Mode.QDEFLATE.name(), new QatDeflateCodec(accelerationMode));
            codecs.put(Lucene99QatCodec.Mode.QLZ4.name(), new QatLz4Codec(accelerationMode));
        } else {
            codecs.put(
                Lucene99QatCodec.Mode.QDEFLATE.name(),
                new PerFieldMappingPostingFormatCodec(Lucene99QatCodec.Mode.QDEFLATE, accelerationMode, mapperService)
            );
            codecs.put(
                Lucene99QatCodec.Mode.QLZ4.name(),
                new PerFieldMappingPostingFormatCodec(Lucene99QatCodec.Mode.QLZ4, accelerationMode, mapperService)
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
