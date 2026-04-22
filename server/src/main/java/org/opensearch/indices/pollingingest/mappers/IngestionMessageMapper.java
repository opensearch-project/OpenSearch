/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest.mappers;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.Message;
import org.opensearch.indices.pollingingest.ShardUpdateMessage;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;

/**
 * Interface for mapping ingestion messages to ShardUpdateMessage format.
 * Different implementations can support different message formats from streaming sources.
 *
 * <p>Note that IngestionMessageMapper will only map the incoming message to a {@link ShardUpdateMessage} and will not
 * validate and drop messages. Validations will be done as part of message processing in the {@link org.opensearch.indices.pollingingest.MessageProcessorRunnable}</p>
 */
public interface IngestionMessageMapper {

    /** Operation type value for index operations */
    String OP_TYPE_INDEX = "index";
    /** Operation type value for delete operations */
    String OP_TYPE_DELETE = "delete";
    /** Operation type value for create operations */
    String OP_TYPE_CREATE = "create";

    /**
     * Maps and processes an ingestion message to a shard update message.
     *
     * @param pointer the shard pointer for this message
     * @param message the message from the streaming source
     * @return the shard update message
     * @throws IllegalArgumentException if the message format is invalid
     */
    ShardUpdateMessage mapAndProcess(IngestionShardPointer pointer, Message message) throws IllegalArgumentException;

    /**
     * Enum representing different mapper types.
     */
    @PublicApi(since = "3.6.0")
    enum MapperType {
        DEFAULT("default"),
        RAW_PAYLOAD("raw_payload"),
        FIELD_MAPPING("field_mapping");

        private final String name;

        MapperType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public static MapperType fromString(String value) {
            for (MapperType type : MapperType.values()) {
                if (type.name.equalsIgnoreCase(value)) {
                    return type;
                }
            }
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Unknown ingestion mapper type: %s. Valid values are: default, raw_payload, field_mapping",
                    value
                )
            );
        }
    }

    /**
     * Validates mapper settings for the given mapper type.
     *
     * @param mapperType the mapper type
     * @param mapperSettings the mapper settings to validate
     * @throws IllegalArgumentException if validation fails
     */
    static void validateSettings(MapperType mapperType, Map<String, Object> mapperSettings) {
        switch (mapperType) {
            case FIELD_MAPPING:
                FieldMappingIngestionMessageMapper.validateSettings(mapperSettings);
                break;
            default:
                if (mapperSettings != null && mapperSettings.isEmpty() == false) {
                    throw new IllegalArgumentException("mapper_settings are not supported for mapper_type [" + mapperType.getName() + "]");
                }
                break;
        }
    }

    /**
     * Factory method to create a mapper instance based on type string.
     *
     * @param mapperTypeString the type of mapper to create as a string
     * @param shardId the shard ID
     * @return the mapper instance
     */
    static IngestionMessageMapper create(String mapperTypeString, int shardId) {
        return create(mapperTypeString, shardId, Collections.emptyMap());
    }

    /**
     * Factory method to create a mapper instance based on type string and mapper settings.
     *
     * @param mapperTypeString the type of mapper to create as a string
     * @param shardId the shard ID
     * @param mapperSettings mapper-specific settings
     * @return the mapper instance
     */
    static IngestionMessageMapper create(String mapperTypeString, int shardId, Map<String, Object> mapperSettings) {
        MapperType mapperType = MapperType.fromString(mapperTypeString);
        switch (mapperType) {
            case DEFAULT:
                return new DefaultIngestionMessageMapper();
            case RAW_PAYLOAD:
                return new RawPayloadIngestionMessageMapper(shardId);
            case FIELD_MAPPING:
                return new FieldMappingIngestionMessageMapper(mapperSettings);
            default:
                throw new IllegalArgumentException("Unknown mapper type: " + mapperType);
        }
    }
}
