/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.codec.composite.composite912.Composite912Codec;
import org.opensearch.index.codec.composite.composite99.Composite99Codec;
import org.opensearch.index.mapper.MapperService;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.index.codec.CodecService.BEST_COMPRESSION_CODEC;
import static org.opensearch.index.codec.CodecService.DEFAULT_CODEC;
import static org.opensearch.index.codec.CodecService.LZ4;
import static org.opensearch.index.codec.CodecService.ZLIB;

/**
 * Factory class to return the latest composite codec for all the modes
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeCodecFactory {

    // we can use this to track the latest composite codec
    public static final String COMPOSITE_CODEC = Composite99Codec.COMPOSITE_INDEX_CODEC_NAME;

    public CompositeCodecFactory() {}

    public Map<String, Codec> getCompositeIndexCodecs(MapperService mapperService, Logger logger) {
        Map<String, Codec> codecs = new HashMap<>();
        codecs.put(DEFAULT_CODEC, new Composite912Codec(Lucene912Codec.Mode.BEST_SPEED, mapperService, logger));
        codecs.put(LZ4, new Composite912Codec(Lucene912Codec.Mode.BEST_SPEED, mapperService, logger));
        codecs.put(BEST_COMPRESSION_CODEC, new Composite912Codec(Lucene912Codec.Mode.BEST_COMPRESSION, mapperService, logger));
        codecs.put(ZLIB, new Composite912Codec(Lucene912Codec.Mode.BEST_COMPRESSION, mapperService, logger));
        return codecs;
    }
}
