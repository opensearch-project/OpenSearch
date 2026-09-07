/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.fields.plugins;

import org.opensearch.be.lucene.fields.LuceneField;
import org.opensearch.be.lucene.fields.core.data.BinaryLuceneField;
import org.opensearch.be.lucene.fields.core.data.BooleanLuceneField;
import org.opensearch.be.lucene.fields.core.data.date.DateLuceneField;
import org.opensearch.be.lucene.fields.core.data.date.DateNanosLuceneField;
import org.opensearch.be.lucene.fields.core.data.number.ByteLuceneField;
import org.opensearch.be.lucene.fields.core.data.number.DoubleLuceneField;
import org.opensearch.be.lucene.fields.core.data.number.FloatLuceneField;
import org.opensearch.be.lucene.fields.core.data.number.HalfFloatLuceneField;
import org.opensearch.be.lucene.fields.core.data.number.IntegerLuceneField;
import org.opensearch.be.lucene.fields.core.data.number.LongLuceneField;
import org.opensearch.be.lucene.fields.core.data.number.ShortLuceneField;
import org.opensearch.be.lucene.fields.core.data.number.TokenCountLuceneField;
import org.opensearch.be.lucene.fields.core.data.number.UnsignedLongLuceneField;
import org.opensearch.be.lucene.fields.core.data.text.IpLuceneField;
import org.opensearch.be.lucene.fields.core.data.text.KeywordLuceneField;
import org.opensearch.be.lucene.fields.core.data.text.TextLuceneField;
import org.opensearch.index.mapper.BinaryFieldMapper;
import org.opensearch.index.mapper.BooleanFieldMapper;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.IpFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;

import java.util.HashMap;
import java.util.Map;

/**
 * Core data fields plugin providing Lucene field implementations for all built-in OpenSearch field types.
 */
public class CoreDataFieldPlugin implements LuceneFieldPlugin {

    /** Creates a new CoreDataFieldPlugin. */
    public CoreDataFieldPlugin() {}

    @Override
    public Map<String, LuceneField> getLuceneFields() {
        final Map<String, LuceneField> fieldMap = new HashMap<>();
        registerNumericFields(fieldMap);
        registerTemporalFields(fieldMap);
        registerBooleanFields(fieldMap);
        registerTextFields(fieldMap);
        registerBinaryFields(fieldMap);
        return fieldMap;
    }

    private static void registerNumericFields(Map<String, LuceneField> fieldMap) {
        fieldMap.put(NumberFieldMapper.NumberType.HALF_FLOAT.typeName(), new HalfFloatLuceneField());
        fieldMap.put(NumberFieldMapper.NumberType.FLOAT.typeName(), new FloatLuceneField());
        fieldMap.put(NumberFieldMapper.NumberType.DOUBLE.typeName(), new DoubleLuceneField());
        fieldMap.put(NumberFieldMapper.NumberType.BYTE.typeName(), new ByteLuceneField());
        fieldMap.put(NumberFieldMapper.NumberType.SHORT.typeName(), new ShortLuceneField());
        fieldMap.put(NumberFieldMapper.NumberType.INTEGER.typeName(), new IntegerLuceneField());
        fieldMap.put(NumberFieldMapper.NumberType.LONG.typeName(), new LongLuceneField());
        fieldMap.put(NumberFieldMapper.NumberType.UNSIGNED_LONG.typeName(), new UnsignedLongLuceneField());
        fieldMap.put("token_count", new TokenCountLuceneField());
        fieldMap.put("scaled_float", new LongLuceneField());
    }

    private static void registerTemporalFields(Map<String, LuceneField> fieldMap) {
        fieldMap.put(DateFieldMapper.CONTENT_TYPE, new DateLuceneField());
        fieldMap.put(DateFieldMapper.DATE_NANOS_CONTENT_TYPE, new DateNanosLuceneField());
    }

    private static void registerBooleanFields(Map<String, LuceneField> fieldMap) {
        fieldMap.put(BooleanFieldMapper.CONTENT_TYPE, new BooleanLuceneField());
    }

    private static void registerTextFields(Map<String, LuceneField> fieldMap) {
        fieldMap.put(TextFieldMapper.CONTENT_TYPE, new TextLuceneField());
        fieldMap.put(KeywordFieldMapper.CONTENT_TYPE, new KeywordLuceneField());
        fieldMap.put(IpFieldMapper.CONTENT_TYPE, new IpLuceneField());
    }

    private static void registerBinaryFields(Map<String, LuceneField> fieldMap) {
        fieldMap.put(BinaryFieldMapper.CONTENT_TYPE, new BinaryLuceneField());
    }
}
