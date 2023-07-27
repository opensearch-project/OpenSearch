/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.codec;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene95.Lucene95Codec;
import org.apache.lucene.codecs.lucene95.Lucene95Codec.Mode;
import org.opensearch.common.Nullable;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.customcodecs.ZstdCodec;
import org.opensearch.index.codec.customcodecs.ZstdNoDictCodec;
import org.opensearch.index.mapper.MapperService;

import java.util.Map;

import static org.opensearch.index.engine.EngineConfig.INDEX_CODEC_COMPRESSION_LEVEL_SETTING;

/**
 * Since Lucene 4.0 low level index segments are read and written through a
 * codec layer that allows to use use-case specific file formats &amp;
 * data-structures per field. OpenSearch exposes the full
 * {@link Codec} capabilities through this {@link CodecService}.
 *
 * @opensearch.internal
 */
public class CodecService {

    private final Map<String, Codec> codecs;

    public static final String DEFAULT_CODEC = "default";
    public static final String BEST_COMPRESSION_CODEC = "best_compression";
    /**
     * the raw unfiltered lucene default. useful for testing
     */
    public static final String LUCENE_DEFAULT_CODEC = "lucene_default";
    public static final String ZSTD_CODEC = "zstd";
    public static final String ZSTD_NO_DICT_CODEC = "zstd_no_dict";

    public CodecService(@Nullable MapperService mapperService, IndexSettings indexSettings, Logger logger) {
        final MapBuilder<String, Codec> codecs = MapBuilder.<String, Codec>newMapBuilder();
        assert null != indexSettings;
        int compressionLevel = indexSettings.getValue(INDEX_CODEC_COMPRESSION_LEVEL_SETTING);
        if (mapperService == null) {
            codecs.put(DEFAULT_CODEC, new Lucene95Codec());
            codecs.put(BEST_COMPRESSION_CODEC, new Lucene95Codec(Mode.BEST_COMPRESSION));
            codecs.put(ZSTD_CODEC, new ZstdCodec(compressionLevel));
            codecs.put(ZSTD_NO_DICT_CODEC, new ZstdNoDictCodec(compressionLevel));
        } else {
            codecs.put(DEFAULT_CODEC, new PerFieldMappingPostingFormatCodec(Mode.BEST_SPEED, mapperService, logger));
            codecs.put(BEST_COMPRESSION_CODEC, new PerFieldMappingPostingFormatCodec(Mode.BEST_COMPRESSION, mapperService, logger));
            codecs.put(ZSTD_CODEC, new ZstdCodec(mapperService, logger, compressionLevel));
            codecs.put(ZSTD_NO_DICT_CODEC, new ZstdNoDictCodec(mapperService, logger, compressionLevel));
        }
        codecs.put(LUCENE_DEFAULT_CODEC, Codec.getDefault());
        for (String codec : Codec.availableCodecs()) {
            codecs.put(codec, Codec.forName(codec));
        }
        this.codecs = codecs.immutableMap();
    }

    public Codec codec(String name) {
        Codec codec = codecs.get(name);
        if (codec == null) {
            throw new IllegalArgumentException("failed to find codec [" + name + "]");
        }
        return codec;
    }

    /**
     * Returns all registered available codec names
     */
    public String[] availableCodecs() {
        return codecs.keySet().toArray(new String[0]);
    }
}
