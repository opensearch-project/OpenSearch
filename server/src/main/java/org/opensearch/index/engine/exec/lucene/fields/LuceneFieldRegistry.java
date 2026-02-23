/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.fields;

import org.opensearch.index.engine.exec.lucene.fields.core.data.BooleanLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.core.data.KeywordLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.core.data.TextLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.core.data.date.DateLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.core.data.number.ByteLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.core.data.number.DoubleLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.core.data.number.FloatLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.core.data.number.HalfFloatLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.core.data.number.IntegerLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.core.data.number.LongLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.core.data.number.ShortLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.core.data.number.UnsignedLongLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.core.metadata.IdLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.core.metadata.SeqNoLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.core.metadata.VersionLuceneField;
import org.opensearch.index.mapper.BooleanFieldMapper;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LuceneFieldRegistry {

    /**
     * All registered field mappings (thread-safe, mutable)
     */
    private static final Map<String, LuceneField> FIELD_REGISTRY = new ConcurrentHashMap<>();

    // Static initialization block to populate the field registry
    static {
        initialize();
    }

    // Private constructor to prevent instantiation of utility class
    private LuceneFieldRegistry() {
        throw new UnsupportedOperationException("Registry class should not be instantiated");
    }

    /**
     * Initialize the registry with all available plugins.
     * This method should be called during node startup after all plugins are loaded.
     */
    public static synchronized void initialize() {
        FIELD_REGISTRY.put(BooleanFieldMapper.CONTENT_TYPE, new BooleanLuceneField());
        FIELD_REGISTRY.put(KeywordFieldMapper.CONTENT_TYPE, new KeywordLuceneField());

        FIELD_REGISTRY.put(NumberFieldMapper.NumberType.HALF_FLOAT.typeName(), new HalfFloatLuceneField());
        FIELD_REGISTRY.put(NumberFieldMapper.NumberType.FLOAT.typeName(), new FloatLuceneField());
        FIELD_REGISTRY.put(NumberFieldMapper.NumberType.DOUBLE.typeName(), new DoubleLuceneField());
        FIELD_REGISTRY.put(NumberFieldMapper.NumberType.BYTE.typeName(), new ByteLuceneField());
        FIELD_REGISTRY.put(NumberFieldMapper.NumberType.SHORT.typeName(), new ShortLuceneField());
        FIELD_REGISTRY.put(NumberFieldMapper.NumberType.INTEGER.typeName(), new IntegerLuceneField());
        FIELD_REGISTRY.put(NumberFieldMapper.NumberType.LONG.typeName(), new LongLuceneField());
        FIELD_REGISTRY.put(NumberFieldMapper.NumberType.UNSIGNED_LONG.typeName(), new UnsignedLongLuceneField());

        FIELD_REGISTRY.put(DateFieldMapper.CONTENT_TYPE, new DateLuceneField());
        FIELD_REGISTRY.put(TextFieldMapper.CONTENT_TYPE, new TextLuceneField());
        FIELD_REGISTRY.put(IdFieldMapper.CONTENT_TYPE, new IdLuceneField());
        FIELD_REGISTRY.put(SeqNoFieldMapper.CONTENT_TYPE, new SeqNoLuceneField());
        FIELD_REGISTRY.put(VersionFieldMapper.CONTENT_TYPE, new VersionLuceneField());
    }

    /**
     * Returns the LuceneField implementation for the specified OpenSearch field type, or null if not found.
     */
    public static LuceneField getLuceneField(String fieldType) {
        return FIELD_REGISTRY.get(fieldType);
    }
}
