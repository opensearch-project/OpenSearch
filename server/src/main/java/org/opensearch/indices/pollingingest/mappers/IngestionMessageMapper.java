/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest.mappers;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.Message;
import org.opensearch.indices.pollingingest.ShardUpdateMessage;

import java.util.Locale;

/**
 * Interface for mapping ingestion messages to ShardUpdateMessage format.
 * Different implementations can support different message formats from streaming sources.
 *
 * <p>Note that IngestionMessageMapper will only map the incoming message to a {@link ShardUpdateMessage} and will not
 * validate and drop messages. Validations will be done as part of message processing in the {@link org.opensearch.indices.pollingingest.MessageProcessorRunnable}</p>
 */
public interface IngestionMessageMapper {

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
    @ExperimentalApi
    enum MapperType {
        DEFAULT("default"),
        RAW_PAYLOAD("raw_payload");

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
                String.format(Locale.ROOT, "Unknown ingestion mapper type: %s. Valid values are: default, raw_payload", value)
            );
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
        MapperType mapperType = MapperType.fromString(mapperTypeString);
        switch (mapperType) {
            case DEFAULT:
                return new DefaultIngestionMessageMapper();
            case RAW_PAYLOAD:
                return new RawPayloadIngestionMessageMapper(shardId);
            default:
                throw new IllegalArgumentException("Unknown mapper type: " + mapperType);
        }
    }
}
