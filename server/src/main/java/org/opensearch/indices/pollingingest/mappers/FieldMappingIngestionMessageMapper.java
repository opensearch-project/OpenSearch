/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest.mappers;

import org.opensearch.common.Nullable;
import org.opensearch.common.util.RequestUtils;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.Message;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.indices.pollingingest.IngestionUtils;
import org.opensearch.indices.pollingingest.ShardUpdateMessage;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.opensearch.action.index.IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP;
import static org.opensearch.indices.pollingingest.MessageProcessorRunnable.ID;
import static org.opensearch.indices.pollingingest.MessageProcessorRunnable.OP_TYPE;
import static org.opensearch.indices.pollingingest.MessageProcessorRunnable.SOURCE;

/**
 * Mapper implementation that extracts document metadata ({@code _id}, {@code _version}, {@code _op_type})
 * from configurable top-level fields in the raw message payload. The remaining fields become the document
 * {@code _source}.
 *
 * <p>Mapper settings:
 * <ul>
 *   <li>{@code id_field} — source field to use as document {@code _id}. If absent, ID is auto-generated.</li>
 *   <li>{@code version_field} — source field to use as document {@code _version} with external versioning.
 *       If configured, the field must be present in every message.</li>
 *   <li>{@code op_type_field} — source field to determine operation type.</li>
 *   <li>{@code op_type_field.delete_value} — the value of {@code op_type_field} that indicates a delete operation.</li>
 *   <li>{@code op_type_field.create_value} — the value of {@code op_type_field} that indicates a create operation.</li>
 * </ul>
 *
 * <p>Operation type resolution: if the field value matches {@code delete_value} → delete,
 * if it matches {@code create_value} → create, otherwise → index (default).
 */
public class FieldMappingIngestionMessageMapper implements IngestionMessageMapper {

    /** Mapper setting key: source field to use as document _id */
    public static final String ID_FIELD = "id_field";
    /** Mapper setting key: source field to use as document _version */
    public static final String VERSION_FIELD = "version_field";
    /** Mapper setting key: source field to determine operation type */
    public static final String OP_TYPE_FIELD = "op_type_field";
    /** Mapper setting key: the value of op_type_field that indicates a delete operation */
    public static final String DELETE_VALUE = "op_type_field.delete_value";
    /** Mapper setting key: the value of op_type_field that indicates a create operation */
    public static final String CREATE_VALUE = "op_type_field.create_value";

    /** Valid mapper_settings keys for this mapper type */
    public static final Set<String> VALID_SETTINGS = Set.of(ID_FIELD, VERSION_FIELD, OP_TYPE_FIELD, DELETE_VALUE, CREATE_VALUE);

    @Nullable
    private final String idField;
    @Nullable
    private final String versionField;
    @Nullable
    private final String opTypeField;
    @Nullable
    private final String deleteValue;
    @Nullable
    private final String createValue;

    /**
     * Creates a FieldMappingIngestionMessageMapper from mapper settings.
     *
     * @param mapperSettings the mapper settings map
     */
    public FieldMappingIngestionMessageMapper(Map<String, Object> mapperSettings) {
        validateSettings(mapperSettings);
        this.idField = mapperSettings != null ? (String) mapperSettings.get(ID_FIELD) : null;
        this.versionField = mapperSettings != null ? (String) mapperSettings.get(VERSION_FIELD) : null;
        this.opTypeField = mapperSettings != null ? (String) mapperSettings.get(OP_TYPE_FIELD) : null;
        this.deleteValue = mapperSettings != null ? (String) mapperSettings.get(DELETE_VALUE) : null;
        this.createValue = mapperSettings != null ? (String) mapperSettings.get(CREATE_VALUE) : null;
    }

    /**
     * Validates mapper settings for the field_mapping mapper type.
     * Checks that all keys are recognized, delete/create values require op_type_field,
     * and delete and create values are not the same.
     *
     * @param mapperSettings the mapper settings to validate
     * @throws IllegalArgumentException if validation fails
     */
    public static void validateSettings(Map<String, Object> mapperSettings) {
        if (mapperSettings == null || mapperSettings.isEmpty()) {
            return;
        }
        // Validate keys
        for (String key : mapperSettings.keySet()) {
            if (VALID_SETTINGS.contains(key) == false) {
                throw new IllegalArgumentException(
                    "unknown mapper_settings key [" + key + "] for mapper_type [field_mapping]. Valid keys are: " + VALID_SETTINGS
                );
            }
        }
        // Validate cross-key dependencies
        boolean hasOpTypeField = mapperSettings.containsKey(OP_TYPE_FIELD);
        String deleteVal = (String) mapperSettings.get(DELETE_VALUE);
        String createVal = (String) mapperSettings.get(CREATE_VALUE);
        if ((deleteVal != null || createVal != null) && hasOpTypeField == false) {
            throw new IllegalArgumentException(
                "op_type_field.delete_value or op_type_field.create_value requires op_type_field to be configured"
            );
        }
        if (deleteVal != null && createVal != null && deleteVal.equals(createVal)) {
            throw new IllegalArgumentException(
                "op_type_field.delete_value [" + deleteVal + "] and op_type_field.create_value [" + createVal + "] cannot be the same"
            );
        }
    }

    @Override
    public ShardUpdateMessage mapAndProcess(IngestionShardPointer pointer, Message message) throws IllegalArgumentException {
        Map<String, Object> rawPayload = IngestionUtils.getParsedPayloadMap((byte[]) message.getPayload());
        Map<String, Object> payloadMap = new HashMap<>();

        // Extract _id
        long autoGeneratedIdTimestamp = UNSET_AUTO_GENERATED_TIMESTAMP;
        if (idField != null) {
            if (rawPayload.containsKey(idField) == false) {
                throw new IllegalArgumentException("configured id_field [" + idField + "] is missing from the message");
            }
            Object idValue = rawPayload.remove(idField);
            validateScalar(idField, idValue);
            String idStr = String.valueOf(idValue).trim();
            if (idStr.isEmpty()) {
                throw new IllegalArgumentException("field [" + idField + "] has an empty value");
            }
            payloadMap.put(ID, idStr);
        } else {
            String id = RequestUtils.generateID();
            payloadMap.put(ID, id);
            autoGeneratedIdTimestamp = System.currentTimeMillis();
        }

        // Extract _version
        if (versionField != null) {
            if (rawPayload.containsKey(versionField) == false) {
                throw new IllegalArgumentException("configured version_field [" + versionField + "] is missing from the message");
            }
            Object versionValue = rawPayload.remove(versionField);
            validateScalar(versionField, versionValue);
            payloadMap.put(VersionFieldMapper.NAME, String.valueOf(versionValue).trim());
        }

        // Extract _op_type
        // The op_type_field value is matched against delete_value and create_value to determine the operation type.
        // If neither delete_value nor create_value is configured, the field is still removed from _source but
        // the operation type defaults to index.
        if (opTypeField != null && rawPayload.containsKey(opTypeField)) {
            Object val = rawPayload.remove(opTypeField);
            validateScalar(opTypeField, val);
            String stringVal = String.valueOf(val).trim();
            payloadMap.put(OP_TYPE, resolveOpType(stringVal));
        } else {
            payloadMap.put(OP_TYPE, OP_TYPE_INDEX);
        }

        // Remaining fields become _source
        payloadMap.put(SOURCE, rawPayload);

        return new ShardUpdateMessage(pointer, message, payloadMap, autoGeneratedIdTimestamp);
    }

    /**
     * Resolves the operation type from the field value.
     * If the value matches delete_value → delete, if it matches create_value → create, otherwise → index.
     */
    private String resolveOpType(String fieldValue) {
        if (deleteValue != null && deleteValue.equals(fieldValue)) {
            return OP_TYPE_DELETE;
        }
        if (createValue != null && createValue.equals(fieldValue)) {
            return OP_TYPE_CREATE;
        }
        return OP_TYPE_INDEX;
    }

    /**
     * Validates that a field value is a non-null scalar type (string, number, or boolean) and not a complex type like
     * a nested object or array.
     */
    private static void validateScalar(String fieldName, Object value) {
        if (value == null) {
            throw new IllegalArgumentException("field [" + fieldName + "] has a null value");
        }
        if (!(value instanceof String) && !(value instanceof Number) && !(value instanceof Boolean)) {
            throw new IllegalArgumentException(
                "field ["
                    + fieldName
                    + "] must be a scalar value (string, number, or boolean), but found: "
                    + value.getClass().getSimpleName()
            );
        }
    }
}
