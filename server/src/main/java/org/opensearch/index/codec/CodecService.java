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
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.composite.CompositeCodecFactory;
import org.opensearch.index.mapper.MapperService;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Since Lucene 4.0 low level index segments are read and written through a
 * codec layer that allows to use use-case specific file formats &amp;
 * data-structures per field. OpenSearch exposes the full
 * {@link Codec} capabilities through this {@link CodecService}.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.3.0")
public class CodecService {

    private final Map<String, Codec> codecs;

    public static final String DEFAULT_CODEC = "default";
    public static final String LZ4 = "lz4";
    public static final String BEST_COMPRESSION_CODEC = "best_compression";
    public static final String ZLIB = "zlib";
    /**
     * the raw unfiltered lucene default. useful for testing
     */
    public static final String LUCENE_DEFAULT_CODEC = "lucene_default";
    private final CompositeCodecFactory compositeCodecFactory = new CompositeCodecFactory();

    /**
     * @deprecated Please use {@code CodecService(MapperService, IndexSettings, Logger, Collection<CodecRegistry>)}
     */
    @Deprecated(forRemoval = true)
    public CodecService(@Nullable MapperService mapperService, IndexSettings indexSettings, Logger logger) {
        this(mapperService, indexSettings, logger, List.of());
    }

    public CodecService(
        @Nullable MapperService mapperService,
        IndexSettings indexSettings,
        Logger logger,
        Collection<AdditionalCodecs> registries
    ) {
        final MapBuilder<String, Codec> codecs = MapBuilder.<String, Codec>newMapBuilder();
        assert null != indexSettings;
        if (mapperService == null) {
            codecs.put(DEFAULT_CODEC, new Lucene103Codec());
            codecs.put(LZ4, new Lucene103Codec());
            codecs.put(BEST_COMPRESSION_CODEC, new Lucene103Codec(Lucene103Codec.Mode.BEST_COMPRESSION));
            codecs.put(ZLIB, new Lucene103Codec(Lucene103Codec.Mode.BEST_COMPRESSION));
        } else {
            // CompositeCodec still delegates to PerFieldMappingPostingFormatCodec
            // We can still support all the compression codecs when composite index is present
            if (mapperService.isCompositeIndexPresent()) {
                codecs.putAll(compositeCodecFactory.getCompositeIndexCodecs(mapperService, logger));
            } else {
                codecs.put(DEFAULT_CODEC, new PerFieldMappingPostingFormatCodec(Lucene103Codec.Mode.BEST_SPEED, mapperService, logger));
                codecs.put(LZ4, new PerFieldMappingPostingFormatCodec(Lucene103Codec.Mode.BEST_SPEED, mapperService, logger));
                codecs.put(
                    BEST_COMPRESSION_CODEC,
                    new PerFieldMappingPostingFormatCodec(Lucene103Codec.Mode.BEST_COMPRESSION, mapperService, logger)
                );
                codecs.put(ZLIB, new PerFieldMappingPostingFormatCodec(Lucene103Codec.Mode.BEST_COMPRESSION, mapperService, logger));
            }
        }
        codecs.put(LUCENE_DEFAULT_CODEC, Codec.getDefault());
        for (String codec : Codec.availableCodecs()) {
            codecs.put(codec, Codec.forName(codec));
        }

        // Register all additional codecs (if available)
        final Supplier<Codec> defaultCodec = () -> codecs.get(DEFAULT_CODEC);
        for (AdditionalCodecs registry : registries) {
            final Map<String, Codec> additionalCodecs = registry.getCodecs(mapperService, indexSettings, defaultCodec);
            for (Map.Entry<String, Codec> additionalCodec : additionalCodecs.entrySet()) {
                final String name = additionalCodec.getKey();

                // Default codec could not be changed
                if (name.equalsIgnoreCase(LUCENE_DEFAULT_CODEC) == true) {
                    throw new IllegalStateException("The default codec could not be replaced");
                }

                final Codec existing = codecs.get(name);
                if (existing == null) {
                    codecs.put(name, additionalCodec.getValue());
                } else {
                    throw new IllegalStateException("The codec with name " + name + " is already registered.");
                }
            }
        }

        this.codecs = codecs.immutableMap();
    }

    /**
     * Returns default codec
     */
    public final Codec defaultCodec() {
        return codecs.get(DEFAULT_CODEC);
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
