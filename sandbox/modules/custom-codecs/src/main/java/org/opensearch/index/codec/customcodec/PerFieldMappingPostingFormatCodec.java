/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodec;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.opensearch.index.mapper.CompletionFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;

public class PerFieldMappingPostingFormatCodec extends Lucene92CustomCodec {
    private final Logger logger;
    private final MapperService mapperService;
    private final DocValuesFormat dvFormat = new Lucene90DocValuesFormat();

    public PerFieldMappingPostingFormatCodec(Lucene92CustomCodec.Mode compressionMode, MapperService mapperService, Logger logger) {
        super(compressionMode);
        this.mapperService = mapperService;
        this.logger = logger;
    }

    @Override
    public PostingsFormat getPostingsFormatForField(String field) {
        final MappedFieldType fieldType = mapperService.fieldType(field);
        if (fieldType == null) {
            logger.warn("no index mapper found for field: [{}] returning default postings format", field);
        } else if (fieldType instanceof CompletionFieldMapper.CompletionFieldType) {
            return CompletionFieldMapper.CompletionFieldType.postingsFormat();
        }
        return super.getPostingsFormatForField(field);
    }

    @Override
    public DocValuesFormat getDocValuesFormatForField(String field) {
        return dvFormat;
    }
}
