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
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MetadataFieldMapper;
import org.opensearch.index.mapper.NestedPathFieldMapper;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.parquet.fields.core.data.NestedParquetField;
import org.opensearch.parquet.fields.core.data.number.LongParquetField;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Builds Apache Arrow schemas from OpenSearch MapperService field mappings via a single recursive
 * walk of the mapper tree — non-nested {@code object} mappers flatten into the enclosing scope (exactly
 * as the flat field-name list always has), and each {@code nested} mapper becomes one
 * {@code LIST<STRUCT<...>>} column built from a recursive call scoped to it, via
 * {@link NestedParquetField#buildField(String, List)}. {@code flat_object}'s {@code MAP<Utf8,Utf8>}
 * shape (root-level or inside a nested struct) is built the same way every other type's is, by its own
 * registered {@link ParquetField#buildField(String)} — this class has no flat_object-specific logic.
 */
public final class ArrowSchemaBuilder {

    private static final Logger logger = LogManager.getLogger(ArrowSchemaBuilder.class);

    private ArrowSchemaBuilder() {}

    /**
     * Creates an Arrow Schema from the MapperService.
     * @param mapperService the mapper service containing field mappings
     * TODO - Get the mapping version while creating the schema
     */
    public static Schema getSchema(MapperService mapperService) {
        Objects.requireNonNull(mapperService, "MapperService cannot be null");
        List<Field> fields = new ArrayList<>();
        DocumentMapper documentMapper = mapperService.documentMapperWithAutoCreate().getDocumentMapper();
        if (documentMapper != null) {
            // Metadata field mappers (_id, _seq_no, _version, _routing, _ignored, ...) live only in the
            // document mapper's separate metadata-mapper set, never as children of the object-mapper
            // tree walked below by collectFields — so they're collected here instead, from the flat
            // mapper lookup. None of them are ever inside a nested scope.
            for (Mapper mapper : documentMapper.mappers()) {
                if (mapper instanceof MetadataFieldMapper == false) {
                    continue;
                }
                if (isUnsupportedMetadataField(mapper)) {
                    logger.debug("Skipping unsupported metadata field: [{}] of type [{}]", mapper.name(), mapper.typeName());
                    continue;
                }
                addLeafField(mapper, null, documentMapper, fields);
            }
            collectFields(documentMapper.root(), null, documentMapper, fields);
        }
        // Add row ID field (long)
        LongParquetField longField = new LongParquetField(false);
        fields.add(new Field(DocumentInput.ROW_ID_FIELD, longField.getFieldType(), null));
        fields.add(new Field(SeqNoFieldMapper.PRIMARY_TERM_NAME, new LongParquetField(false).getFieldType(), null));
        return new Schema(fields);
    }

    /**
     * Recursively walks the non-metadata mapper tree rooted at {@code mapper}, appending Arrow fields to
     * {@code outFields}. {@code Mapper}'s {@link Iterable} contract covers every shape with one walk: a
     * plain {@code object}'s children, a {@code nested} object's children (scoped into its own
     * {@code LIST<STRUCT>}), and a field's own multi-fields (via {@link FieldMapper#iterator()}).
     *
     * @param stripPrefix the dotted-path prefix (ending in {@code "."}) of the innermost enclosing
     *                     nested scope, used to relativize leaf names inside it; {@code null} at the
     *                     document root (or inside a plain, non-nested object, which flattens into
     *                     whatever scope encloses IT)
     */
    private static void collectFields(Mapper mapper, String stripPrefix, DocumentMapper documentMapper, List<Field> outFields) {
        for (Mapper child : mapper) {
            if (isUnsupportedMetadataField(child)) {
                logger.debug("Skipping unsupported metadata field: [{}] of type [{}]", child.name(), child.typeName());
                continue;
            }
            if (child instanceof ObjectMapper objectMapper) {
                if (objectMapper.nested().isNested()) {
                    List<Field> nestedChildren = new ArrayList<>();
                    collectFields(objectMapper, objectMapper.fullPath() + ".", documentMapper, nestedChildren);
                    if (nestedChildren.isEmpty() == false) {
                        NestedParquetField nestedField = (NestedParquetField) ArrowFieldRegistry.getParquetField(
                            ObjectMapper.NESTED_CONTENT_TYPE
                        );
                        outFields.add(nestedField.buildField(relativize(objectMapper.fullPath(), stripPrefix), nestedChildren));
                    }
                } else {
                    // A plain object flattens into the SAME enclosing scope — its leaves are relative to
                    // whatever nested scope (if any) already encloses it, not to the object itself.
                    collectFields(objectMapper, stripPrefix, documentMapper, outFields);
                }
            } else if (child instanceof FieldMapper) {
                addLeafField(child, stripPrefix, documentMapper, outFields);
                // Multi-fields (FieldMapper#iterator() == its own MultiFields) are kept as their own flat
                // sibling leaf with a dotted name (e.g. "author.raw" next to "author") — not further
                // nested — exactly like the plain top-level field-name list already flattens them.
                collectFields(child, stripPrefix, documentMapper, outFields);
            }
        }
    }

    /** Builds and appends the Arrow field for one leaf {@code mapper}, plus its normalized-field companion if any. */
    private static void addLeafField(Mapper mapper, String stripPrefix, DocumentMapper documentMapper, List<Field> outFields) {
        ParquetField parquetField = ArrowFieldRegistry.getParquetField(mapper.typeName());
        if (parquetField == null) {
            logger.debug("No ParquetField registered for field: [{}] of type [{}]", mapper.name(), mapper.typeName());
            return;
        }
        outFields.add(parquetField.buildField(relativize(mapper.name(), stripPrefix)));
        if (stripPrefix == null) {
            // A keyword's ignore_above/normalizer raw-value companion column is only ever added at the
            // document root, matching prior behavior — one isn't separately represented for a nested-scope
            // keyword.
            handleNormalizedField(mapper, documentMapper, outFields, parquetField);
        }
    }

    /** Returns {@code fullName} unchanged at the document root ({@code stripPrefix == null}), else relative to the enclosing nested scope. */
    private static String relativize(String fullName, String stripPrefix) {
        return stripPrefix == null ? fullName : fullName.substring(stripPrefix.length());
    }

    private static void handleNormalizedField(Mapper mapper, DocumentMapper documentMapper, List<Field> fields, ParquetField parquetField) {
        if (mapper instanceof KeywordFieldMapper keywordFieldMapper) {
            if (!documentMapper.mappers().isMultiField(mapper.name()) && keywordFieldMapper.getRawValueFieldType() != null) {
                KeywordFieldMapper.KeywordFieldType rawValueField = keywordFieldMapper.getRawValueFieldType();
                fields.add(new Field(rawValueField.name(), parquetField.getFieldType(), null));
            }
        }
    }

    /** Package-visible so tests can apply the same metadata-field exclusions. */
    static boolean isUnsupportedMetadataField(Mapper mapper) {
        return mapper instanceof SourceFieldMapper
            || mapper instanceof FieldNamesFieldMapper
            || mapper instanceof IndexFieldMapper
            || mapper instanceof NestedPathFieldMapper
            || Objects.equals(mapper.typeName(), "_feature")
            || Objects.equals(mapper.typeName(), "_data_stream_timestamp");
    }
}
