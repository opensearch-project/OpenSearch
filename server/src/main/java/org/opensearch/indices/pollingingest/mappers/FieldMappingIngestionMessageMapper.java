/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest.mappers;

import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.Message;
import org.opensearch.indices.pollingingest.ShardUpdateMessage;

import java.util.Set;

/**
 * Mapper implementation that extracts document metadata ({@code _id}, {@code _version}, {@code _op_type})
 * from configurable top-level fields in the raw message payload. The remaining fields become the document
 * {@code _source}.
 *
 * <p>Mapper settings:
 * <ul>
 *   <li>{@code id_field} — source field to use as document {@code _id}. If absent, ID is auto-generated.</li>
 *   <li>{@code version_field} — source field to use as document {@code _version} with external versioning.</li>
 *   <li>{@code op_type_field} — source field (boolean) to determine operation type: {@code true} → delete,
 *       {@code false} → index.</li>
 * </ul>
 */
public class FieldMappingIngestionMessageMapper implements IngestionMessageMapper {

    /** Mapper setting key: source field to use as document _id */
    public static final String ID_FIELD = "id_field";
    /** Mapper setting key: source field to use as document _version */
    public static final String VERSION_FIELD = "version_field";
    /** Mapper setting key: source field to determine operation type (index vs delete) */
    public static final String OP_TYPE_FIELD = "op_type_field";

    /** Valid mapper_settings keys for this mapper type */
    public static final Set<String> VALID_SETTINGS = Set.of(ID_FIELD, VERSION_FIELD, OP_TYPE_FIELD);

    @Override
    public ShardUpdateMessage mapAndProcess(IngestionShardPointer pointer, Message message) throws IllegalArgumentException {
        // TODO: pending implementation
        throw new UnsupportedOperationException("FieldMappingIngestionMessageMapper is not yet implemented");
    }
}
