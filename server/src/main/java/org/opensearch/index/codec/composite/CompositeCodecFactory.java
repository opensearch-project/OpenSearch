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
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.opensearch.common.annotation.ExperimentalApi;
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
    public CompositeCodecFactory() {}

    public Map<String, Codec> getCompositeIndexCodecs(MapperService mapperService, Logger logger) {
        Map<String, Codec> codecs = new HashMap<>();
        codecs.put(DEFAULT_CODEC, new Composite99Codec(Lucene99Codec.Mode.BEST_SPEED, mapperService, logger));
        codecs.put(LZ4, new Composite99Codec(Lucene99Codec.Mode.BEST_SPEED, mapperService, logger));
        codecs.put(BEST_COMPRESSION_CODEC, new Composite99Codec(Lucene99Codec.Mode.BEST_COMPRESSION, mapperService, logger));
        codecs.put(ZLIB, new Composite99Codec(Lucene99Codec.Mode.BEST_COMPRESSION, mapperService, logger));
        return codecs;
    }
}
