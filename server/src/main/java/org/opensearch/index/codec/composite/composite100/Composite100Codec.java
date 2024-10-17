/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite.composite100;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.lucene100.Lucene100Codec;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.codec.PerFieldMappingPostingFormatCodec;
import org.opensearch.index.codec.composite.composite912.Composite912DocValuesFormat;
import org.opensearch.index.mapper.MapperService;

/**
 *  Extends the Codec to support new file formats for composite indices eg: star tree index
 *  based on the mappings.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class Composite100Codec extends FilterCodec {
    public static final String COMPOSITE_INDEX_CODEC_NAME = "Composite100Codec";
    private final MapperService mapperService;

    // needed for SPI - this is used in reader path
    public Composite100Codec() {
        this(COMPOSITE_INDEX_CODEC_NAME, new Lucene100Codec(), null);
    }

    public Composite100Codec(Lucene100Codec.Mode compressionMode, MapperService mapperService, Logger logger) {
        this(COMPOSITE_INDEX_CODEC_NAME, new PerFieldMappingPostingFormatCodec(compressionMode, mapperService, logger), mapperService);
    }

    /**
     * Sole constructor. When subclassing this codec, create a no-arg ctor and pass the delegate codec and a unique name to
     * this ctor.
     *
     * @param name name of the codec
     * @param delegate codec delegate
     * @param mapperService mapper service instance
     */
    protected Composite100Codec(String name, Codec delegate, MapperService mapperService) {
        super(name, delegate);
        this.mapperService = mapperService;
    }

    @Override
    public DocValuesFormat docValuesFormat() {
        return new Composite912DocValuesFormat(mapperService);
    }
}
