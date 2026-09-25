/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.fields.plugins;

import org.opensearch.be.lucene.fields.LuceneField;
import org.opensearch.be.lucene.fields.core.data.number.LongLuceneField;
import org.opensearch.be.lucene.fields.core.metadata.IdLuceneField;
import org.opensearch.be.lucene.fields.core.metadata.IgnoredLuceneField;
import org.opensearch.be.lucene.fields.core.metadata.RoutingLuceneField;
import org.opensearch.be.lucene.fields.core.metadata.SizeLuceneField;
import org.opensearch.index.mapper.DocCountFieldMapper;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.IgnoredFieldMapper;
import org.opensearch.index.mapper.RoutingFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;

import java.util.HashMap;
import java.util.Map;

/**
 * Metadata fields plugin providing Lucene field implementations for OpenSearch metadata fields.
 */
public class MetadataFieldPlugin implements LuceneFieldPlugin {

    /** Creates a new MetadataFieldPlugin. */
    public MetadataFieldPlugin() {}

    @Override
    public Map<String, LuceneField> getLuceneFields() {
        final Map<String, LuceneField> fieldMap = new HashMap<>();
        fieldMap.put(DocCountFieldMapper.CONTENT_TYPE, new LongLuceneField());
        fieldMap.put("_size", new SizeLuceneField());
        fieldMap.put(RoutingFieldMapper.CONTENT_TYPE, new RoutingLuceneField());
        fieldMap.put(IgnoredFieldMapper.CONTENT_TYPE, new IgnoredLuceneField());
        fieldMap.put(IdFieldMapper.CONTENT_TYPE, new IdLuceneField());
        fieldMap.put(SeqNoFieldMapper.CONTENT_TYPE, new LongLuceneField());
        fieldMap.put(SeqNoFieldMapper.PRIMARY_TERM_NAME, new LongLuceneField());
        fieldMap.put(VersionFieldMapper.CONTENT_TYPE, new LongLuceneField());
        return fieldMap;
    }
}
