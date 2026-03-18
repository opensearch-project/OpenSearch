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
import org.opensearch.be.lucene.LuceneBackendPlugin;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.index.mapper.MapperService;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Central registry of all supported {@link PredicateHandler} implementations.
 *
 * <p>This is the single place to register new Lucene predicate types.
 * All downstream components ({@link RexToQueryBuilderConverter},
 * {@link QueryBuilderSerializer}, {@link LuceneBackendPlugin}) derive
 * their behavior from this registry.
 */
public final class PredicateHandlerRegistry {

    private static final List<PredicateHandler> HANDLERS = List.of(
        new EqualsPredicateHandler(),
        new LikePredicateHandler()
        // To add a new predicate: implement PredicateHandler and add it here.
        // Example: new RangePredicateHandler(), new RegexPredicateHandler()
    );

    private static final Map<SqlKind, PredicateHandler> BY_KIND;

    static {
        Map<SqlKind, PredicateHandler> map = new EnumMap<>(SqlKind.class);
        for (PredicateHandler handler : HANDLERS) {
            PredicateHandler prev = map.put(handler.sqlKind(), handler);
            if (prev != null) {
                throw new IllegalStateException(
                    "Duplicate PredicateHandler for SqlKind." + handler.sqlKind() + ": " + prev.getClass().getSimpleName()
                        + " and " + handler.getClass().getSimpleName()
                );
            }
        }
        BY_KIND = Collections.unmodifiableMap(map);
    }

    private PredicateHandlerRegistry() {}

    /** Returns the handler for the given SqlKind, or null if unsupported. */
    public static PredicateHandler getHandler(SqlKind kind) {
        return BY_KIND.get(kind);
    }

    /**
     * Returns true if a registered handler can process the given RexCall.
     * Checks both SqlKind match and handler-specific validation via {@link PredicateHandler#canHandle}.
     */
    public static boolean canHandle(RexCall call, RelDataType inputRowType, MapperService mapperService) {
        PredicateHandler handler = BY_KIND.get(call.getKind());
        return handler != null && handler.canHandle(call, inputRowType, mapperService);
    }

    /** Returns all registered handlers. */
    public static List<PredicateHandler> allHandlers() {
        return HANDLERS;
    }

    /** Returns all SqlOperators supported by registered handlers. */
    public static List<SqlOperator> allOperators() {
        return HANDLERS.stream().map(PredicateHandler::sqlOperator).collect(Collectors.toList());
    }

    /** Returns all NamedWriteableRegistry entries needed by registered handlers. */
    public static List<NamedWriteableRegistry.Entry> allNamedWriteableEntries() {
        return HANDLERS.stream().flatMap(h -> h.namedWriteableEntries().stream()).collect(Collectors.toList());
    }
}
