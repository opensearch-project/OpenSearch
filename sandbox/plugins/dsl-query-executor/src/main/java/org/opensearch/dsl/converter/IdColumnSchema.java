/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.index.mapper.IdFieldMapper;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;

/**
 * DSL's view of the engine schema: every table gains the {@code _id} metadata column
 * (VARBINARY, the stored Uid-encoded bytes), appended after the mapped fields so their
 * ordinals are unchanged.
 *
 * <p>Scans built from this catalog request {@code _id} from the shards the same way QTF
 * requests {@code ___row_id}: the engine resolves storage for whatever field names the scan's
 * row type carries. Only DSL's catalog is wrapped — the shared engine schema is untouched, so
 * PPL's star expansion does not grow an {@code _id} column.
 */
final class IdColumnSchema extends AbstractSchema {

    private final SchemaPlus delegate;

    private IdColumnSchema(SchemaPlus delegate) {
        this.delegate = delegate;
    }

    /** Wraps the engine-provided schema with the {@code _id}-appending view. */
    static Schema wrap(SchemaPlus engineSchema) {
        return new IdColumnSchema(engineSchema);
    }

    @Override
    protected Map<String, Table> getTableMap() {
        // Lazy delegating map: the engine schema resolves index expressions on first lookup.
        return new AbstractMap<>() {
            @Override
            public Table get(Object key) {
                Table table = delegate.getTable((String) key);
                return table == null ? null : withIdColumn(table);
            }

            @Override
            public boolean containsKey(Object key) {
                return get(key) != null;
            }

            @Override
            public Set<Entry<String, Table>> entrySet() {
                return Set.of(); // exact-name lookups only, like the underlying lazy schema
            }
        };
    }

    private static Table withIdColumn(Table table) {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                RelDataType original = table.getRowType(typeFactory);
                if (original.getField(IdFieldMapper.NAME, false, false) != null) {
                    return original;
                }
                RelDataTypeFactory.Builder builder = typeFactory.builder();
                original.getFieldList().forEach(f -> builder.add(f.getName(), f.getType()));
                builder.add(
                    IdFieldMapper.NAME,
                    typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARBINARY), false)
                );
                return builder.build();
            }
        };
    }
}
