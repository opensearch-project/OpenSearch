/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.FieldMapper;
import org.opensearch.index.mapper.FieldNamesFieldMapper;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NestedPathFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.parquet.fields.core.data.number.LongParquetField;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Builds Apache Arrow schemas from OpenSearch MapperService field mappings.
 */
public final class ArrowSchemaBuilder {

    private static final Logger logger = LogManager.getLogger(ArrowSchemaBuilder.class);

    private ArrowSchemaBuilder() {}

    /**
     * Creates an Arrow Schema from the MapperService.
     *
     * <p>A field whose mapper declares {@code multi_value: true}
     * ({@link MappedFieldType#isMultiValued()}) is emitted as a {@code LIST<element>} column;
     * every other field keeps its scalar column.
     * TODO - Get the mapping version while creating the schema
     *
     * @param mapperService the mapper service containing field mappings
     */
    public static Schema getSchema(MapperService mapperService) {
        Objects.requireNonNull(mapperService, "MapperService cannot be null");
        List<Field> fields = new ArrayList<>();
        DocumentMapper documentMapper = mapperService.documentMapperWithAutoCreate().getDocumentMapper();
        if (documentMapper != null) {
            for (Mapper mapper : documentMapper.mappers()) {
                if (isUnsupportedMetadataField(mapper)) {
                    logger.debug("Skipping unsupported metadata field: [{}] of type [{}]", mapper.name(), mapper.typeName());
                    continue;
                }

                ParquetField parquetField = ArrowFieldRegistry.getParquetField(mapper.typeName());
                if (parquetField != null) {
                    boolean multiValue = isMultiValued(mapper);
                    if (multiValue && parquetField.supportsMultiValue() == false) {
                        throw new IllegalArgumentException(
                            "Field ["
                                + mapper.name()
                                + "] of type ["
                                + mapper.typeName()
                                + "] does not support [multi_value] storage in the parquet data format"
                        );
                    }
                    fields.add(parquetField.toArrowField(mapper.name(), multiValue));
                    handleNormalizedField(mapper, documentMapper, fields, parquetField, multiValue);
                } else {
                    logger.debug("No ParquetField registered for field: [{}] of type [{}]", mapper.name(), mapper.typeName());
                }
            }
        }
        // Add row ID field (long)
        LongParquetField longField = new LongParquetField(false);
        fields.add(new Field(DocumentInput.ROW_ID_FIELD, longField.getFieldType(), null));
        fields.add(new Field(SeqNoFieldMapper.PRIMARY_TERM_NAME, new LongParquetField(false).getFieldType(), null));
        return new Schema(fields);
    }

    private static void handleNormalizedField(
        Mapper mapper,
        DocumentMapper documentMapper,
        List<Field> fields,
        ParquetField parquetField,
        boolean multiValue
    ) {
        if (mapper instanceof KeywordFieldMapper keywordFieldMapper) {
            if (!documentMapper.mappers().isMultiField(mapper.name()) && keywordFieldMapper.getRawValueFieldType() != null) {
                KeywordFieldMapper.KeywordFieldType rawValueField = keywordFieldMapper.getRawValueFieldType();
                // The raw-value companion holds the pre-normalization source for derived source, so
                // it must mirror the parent's cardinality or source reconstruction would lose values.
                fields.add(parquetField.toArrowField(rawValueField.name(), multiValue));
            }
        }
    }

    /** Reads the {@code multi_value} declaration from the mapper's field type. */
    private static boolean isMultiValued(Mapper mapper) {
        return mapper instanceof FieldMapper fieldMapper && fieldMapper.fieldType().isMultiValued();
    }

    private static boolean isUnsupportedMetadataField(Mapper mapper) {
        return mapper instanceof FieldNamesFieldMapper
            || mapper instanceof IndexFieldMapper
            || mapper instanceof NestedPathFieldMapper
            || Objects.equals(mapper.typeName(), "_feature")
            || Objects.equals(mapper.typeName(), "_data_stream_timestamp");
    }
}
