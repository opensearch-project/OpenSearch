/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.fields;

import org.opensearch.index.engine.exec.lucene.fields.data.BinaryLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.data.BooleanLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.data.date.DateLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.data.date.DateNanosLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.data.metadata.IdLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.data.metadata.IgnoredLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.data.metadata.RoutingLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.data.metadata.SizeLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.data.number.ByteLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.data.number.DocCountLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.data.number.DoubleLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.data.number.FloatLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.data.number.HalfFloatLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.data.number.IntegerLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.data.number.LongLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.data.number.ShortLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.data.number.TokenCountLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.data.number.UnsignedLongLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.data.text.IpLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.data.text.KeywordLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.data.text.TextLuceneField;
import org.opensearch.index.mapper.BinaryFieldMapper;
import org.opensearch.index.mapper.BooleanFieldMapper;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.DocCountFieldMapper;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.IgnoredFieldMapper;
import org.opensearch.index.mapper.IpFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.RoutingFieldMapper;
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
        // Text-based fields
        FIELD_REGISTRY.put(KeywordFieldMapper.CONTENT_TYPE, new KeywordLuceneField());
        FIELD_REGISTRY.put(TextFieldMapper.CONTENT_TYPE, new TextLuceneField());
        FIELD_REGISTRY.put(IpFieldMapper.CONTENT_TYPE, new IpLuceneField());

        // Numeric fields
        FIELD_REGISTRY.put(NumberFieldMapper.NumberType.BYTE.typeName(), new ByteLuceneField());
        FIELD_REGISTRY.put(NumberFieldMapper.NumberType.SHORT.typeName(), new ShortLuceneField());
        FIELD_REGISTRY.put(NumberFieldMapper.NumberType.INTEGER.typeName(), new IntegerLuceneField());
        FIELD_REGISTRY.put(NumberFieldMapper.NumberType.LONG.typeName(), new LongLuceneField());
        FIELD_REGISTRY.put(NumberFieldMapper.NumberType.UNSIGNED_LONG.typeName(), new UnsignedLongLuceneField());
        FIELD_REGISTRY.put(NumberFieldMapper.NumberType.HALF_FLOAT.typeName(), new HalfFloatLuceneField());
        FIELD_REGISTRY.put(NumberFieldMapper.NumberType.FLOAT.typeName(), new FloatLuceneField());
        FIELD_REGISTRY.put(NumberFieldMapper.NumberType.DOUBLE.typeName(), new DoubleLuceneField());
        FIELD_REGISTRY.put("token_count", new TokenCountLuceneField());
        FIELD_REGISTRY.put("scaled_float", new LongLuceneField());

        // Temporal fields
        FIELD_REGISTRY.put(DateFieldMapper.CONTENT_TYPE, new DateLuceneField());
        FIELD_REGISTRY.put(DateFieldMapper.DATE_NANOS_CONTENT_TYPE, new DateNanosLuceneField());

        // Boolean field
        FIELD_REGISTRY.put(BooleanFieldMapper.CONTENT_TYPE, new BooleanLuceneField());

        // Binary field
        FIELD_REGISTRY.put(BinaryFieldMapper.CONTENT_TYPE, new BinaryLuceneField());

        // Metadata fields
        FIELD_REGISTRY.put(IdFieldMapper.CONTENT_TYPE, new IdLuceneField());
        FIELD_REGISTRY.put(RoutingFieldMapper.CONTENT_TYPE, new RoutingLuceneField());
        FIELD_REGISTRY.put(IgnoredFieldMapper.CONTENT_TYPE, new IgnoredLuceneField());
        FIELD_REGISTRY.put("_size", new SizeLuceneField());
        FIELD_REGISTRY.put(DocCountFieldMapper.CONTENT_TYPE, new DocCountLuceneField());
        FIELD_REGISTRY.put(SeqNoFieldMapper.CONTENT_TYPE, new LongLuceneField());
        FIELD_REGISTRY.put(VersionFieldMapper.CONTENT_TYPE, new LongLuceneField());
        FIELD_REGISTRY.put(SeqNoFieldMapper.PRIMARY_TERM_NAME, new LongLuceneField());
    }

    /**
     * Returns the LuceneField implementation for the specified OpenSearch field type, or null if not found.
     */
    public static LuceneField getLuceneField(String fieldType) {
        return FIELD_REGISTRY.get(fieldType);
    }

    /**
     * Returns all registered field type names.
     */
    public static java.util.Set<String> getRegisteredFieldNames() {
        return java.util.Collections.unmodifiableSet(FIELD_REGISTRY.keySet());
    }

    /**
     * Returns an unmodifiable view of all registered field mappings.
     */
    public static Map<String, LuceneField> getRegisteredFields() {
        return java.util.Collections.unmodifiableMap(FIELD_REGISTRY);
    }

}
