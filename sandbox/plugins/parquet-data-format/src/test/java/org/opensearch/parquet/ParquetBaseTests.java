/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.lucene.search.Query;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatTestUtils;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.parquet.writer.ParquetDocumentInput;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.COLUMNAR_STORAGE;

public abstract class ParquetBaseTests extends OpenSearchTestCase {

    public static final MappedFieldType SEQ_NO_FIELD = new NumberFieldMapper.NumberFieldType(
        SeqNoFieldMapper.NAME,
        NumberFieldMapper.NumberType.LONG
    );

    public static final MappedFieldType PRIMARY_TERM_FIELD = new NumberFieldMapper.NumberFieldType(
        SeqNoFieldMapper.PRIMARY_TERM_NAME,
        NumberFieldMapper.NumberType.LONG
    );

    public static final MappedFieldType VERSION_FIELD = new NumberFieldMapper.NumberFieldType(
        VersionFieldMapper.NAME,
        NumberFieldMapper.NumberType.LONG
    );

    public static final MappedFieldType ID_FIELD = new MappedFieldType(
        IdFieldMapper.CONTENT_TYPE,
        true,
        true,
        true,
        TextSearchInfo.SIMPLE_MATCH_ONLY,
        Map.of()
    ) {
        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            return null;
        }

        @Override
        public String typeName() {
            return IdFieldMapper.CONTENT_TYPE;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            return null;
        }
    };

    static {
        SEQ_NO_FIELD.setCapabilityMap(Map.of(ParquetDataFormatPlugin.PARQUET_DATA_FORMAT, Set.of(COLUMNAR_STORAGE)));
        VERSION_FIELD.setCapabilityMap(Map.of(ParquetDataFormatPlugin.PARQUET_DATA_FORMAT, Set.of(COLUMNAR_STORAGE)));
        ID_FIELD.setCapabilityMap(Map.of(ParquetDataFormatPlugin.PARQUET_DATA_FORMAT, Set.of(COLUMNAR_STORAGE)));
        PRIMARY_TERM_FIELD.setCapabilityMap(Map.of(ParquetDataFormatPlugin.PARQUET_DATA_FORMAT, Set.of(COLUMNAR_STORAGE)));
    }

    public static List<Field> metadataFields() {
        List<Field> fields = new ArrayList<>();
        fields.add(new Field(VersionFieldMapper.NAME, FieldType.notNullable(new ArrowType.Int(64, true)), null));
        fields.add(new Field(SeqNoFieldMapper.NAME, FieldType.notNullable(new ArrowType.Int(64, true)), null));
        fields.add(new Field(SeqNoFieldMapper.PRIMARY_TERM_NAME, FieldType.notNullable(new ArrowType.Int(64, true)), null));
        fields.add(new Field(IdFieldMapper.NAME, FieldType.notNullable(new ArrowType.Binary()), null));
        return fields;
    }

    protected void populateMetadataFields(ParquetDocumentInput input) {
        input.addField(SEQ_NO_FIELD, 100L);
        input.addField(ID_FIELD, "id".getBytes(StandardCharsets.UTF_8));
        input.addField(VERSION_FIELD, 1L);
        input.addField(PRIMARY_TERM_FIELD, 1L);
    }

    protected void assignTestCapabilities(MappedFieldType fieldType, DataFormat format) {
        DataFormatTestUtils.assignTestCapabilities(fieldType, format);
    }

    protected NumberFieldMapper.NumberFieldType createNumberField(String name, NumberFieldMapper.NumberType type) {
        NumberFieldMapper.NumberFieldType numberFieldType = new NumberFieldMapper.NumberFieldType(name, type);
        assignTestCapabilities(numberFieldType, ParquetDataFormatPlugin.PARQUET_DATA_FORMAT);
        return numberFieldType;
    }

    protected KeywordFieldMapper.KeywordFieldType createKeywordField(String name) {
        KeywordFieldMapper.KeywordFieldType keywordFieldType = new KeywordFieldMapper.KeywordFieldType(name);
        assignTestCapabilities(keywordFieldType, ParquetDataFormatPlugin.PARQUET_DATA_FORMAT);
        return keywordFieldType;
    }
}
