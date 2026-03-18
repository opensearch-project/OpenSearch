/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.predicate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.QueryBuilder;

import java.util.List;

/**
 * Extension point for adding new Lucene predicate types.
 *
 * <p>Each handler bundles three concerns that must stay in sync:
 * <ol>
 *   <li>What SQL operator it handles (for planner capability advertisement)</li>
 *   <li>How to convert a Calcite {@link RexCall} into an OpenSearch {@link QueryBuilder}</li>
 *   <li>What {@link NamedWriteableRegistry} entries are needed for serialization</li>
 * </ol>
 *
 * <p>To add a new Lucene predicate type (e.g., range, regex, fuzzy):
 * implement this interface and register it in {@link PredicateHandlerRegistry}.
 */
public interface PredicateHandler {

    /** The Calcite {@link SqlKind} this handler processes (e.g., EQUALS, LIKE). */
    SqlKind sqlKind();

    /** The Calcite {@link SqlOperator} to advertise in the plugin's operator table. */
    SqlOperator sqlOperator();

    /**
     * Converts a {@link RexCall} of the matching {@link #sqlKind()} into a {@link QueryBuilder}.
     *
     * @param call           the Calcite function call to convert
     * @param inputRowType   the row type of the input relation (for resolving field names)
     * @param mapperService  the index mapper service for field type validation, or null if unavailable
     * @return the corresponding QueryBuilder
     * @throws IllegalArgumentException if the call has unsupported operand types
     */
    QueryBuilder convert(RexCall call, RelDataType inputRowType, MapperService mapperService);

    /**
     * Returns true if this handler can process the given {@link RexCall}.
     * Used by the coordinator to validate predicates before conversion.
     *
     * <p>The default implementation returns true for any call matching {@link #sqlKind()}.
     * Handlers that need field-type-aware validation (e.g., only keyword fields)
     * should override this.
     *
     * @param call           the Calcite function call to check
     * @param inputRowType   the row type of the input relation
     * @param mapperService  the index mapper service for field type validation, or null if unavailable
     * @return true if this handler can convert the call
     */
    default boolean canHandle(RexCall call, RelDataType inputRowType, MapperService mapperService) {
        return call.getKind() == sqlKind();
    }

    /**
     * Returns the {@link NamedWriteableRegistry} entries needed to serialize/deserialize
     * the {@link QueryBuilder} types this handler produces.
     */
    List<NamedWriteableRegistry.Entry> namedWriteableEntries();
}
