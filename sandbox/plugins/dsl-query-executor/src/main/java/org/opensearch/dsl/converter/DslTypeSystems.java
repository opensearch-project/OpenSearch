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
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.SqlTypeName;

/** Shared type-system constants: default Calcite clamps TIMESTAMP to precision 3, date_nanos needs 9. */
public final class DslTypeSystems {

    /**
     * The type system every DSL-emitted plan is built with. It differs from
     * {@link RelDataTypeSystem#DEFAULT} in exactly three places, all three because Calcite's default
     * disagrees with what the storage and execution layers actually do:
     *
     * <ol>
     * <li><b>{@code TIMESTAMP} max precision raised to 9</b> so {@code date_nanos} fields are not
     * silently clamped to milliseconds.</li>
     * <li><b>{@code SUM}'s derived type widened to the execution engine's accumulator width</b> — see
     * {@code deriveSumType} below.</li>
     * <li><b>{@code AVG}'s derived type declared {@code DOUBLE}</b> — see {@code deriveAvgAggType} below.
     * The two aggregate overrides are one mechanism: {@code AVG} is executed as a rule-generated
     * {@code SUM}/{@code COUNT}/{@code DIVIDE} plus a CAST back to {@code AVG}'s declared type, so leaving
     * {@code AVG} at its argument's integral width casts away the widening the line above just applied.</li>
     * </ol>
     */
    public static final RelDataTypeSystem NANO_TIMESTAMP = new RelDataTypeSystemImpl() {
        @Override
        public int getMaxPrecision(SqlTypeName typeName) {
            if (typeName == SqlTypeName.TIMESTAMP) {
                return 9;
            }
            return super.getMaxPrecision(typeName);
        }

        /**
         * Widens {@code SUM}'s type to the engine's accumulator width: signed integers to
         * {@code BIGINT} (i64), approximate numerics to {@code DOUBLE} (f64). Nullability is carried
         * over from the argument — dropping it would make {@code SUM} over a nullable column
         * non-nullable and break the empty-group contract Calcite relies on. A type family with no
         * known accumulator widening (decimal, interval, non-numeric) falls through to Calcite's
         * default rather than being guessed at.
         *
         * @param typeFactory the plan's type factory
         * @param argumentType the summed column's type
         * @return the widened sum type, or Calcite's default for a family with no known widening
         */
        @Override
        public RelDataType deriveSumType(RelDataTypeFactory typeFactory, RelDataType argumentType) {
            SqlTypeName widened = switch (argumentType.getSqlTypeName()) {
                case TINYINT, SMALLINT, INTEGER, BIGINT -> SqlTypeName.BIGINT;
                case REAL, FLOAT, DOUBLE -> SqlTypeName.DOUBLE;
                default -> null;
            };
            if (widened == null) {
                return super.deriveSumType(typeFactory, argumentType);
            }
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(widened), argumentType.isNullable());
        }

        /**
         * Declares {@code AVG} (and the other {@code SqlKind#AVG_AGG_FUNCTIONS}) at {@code DOUBLE} for
         * every numeric argument — both the signed-integer and the approximate-numeric family, unlike
         * {@code deriveSumType}'s two targets. Nullability is carried over from the argument for the same
         * reason it is there: a mean over a nullable column really can be null, and the reduced plan the
         * engine executes contains an explicit {@code CASE WHEN count(...) = 0 THEN NULL ...} guard that a
         * NOT NULL declaration would license the constant-reduction rules to fold away. A type family with
         * no known mean type (decimal, interval, non-numeric) falls through to Calcite's default rather
         * than being guessed at, exactly as above.
         * @param typeFactory the plan's type factory
         * @param argumentType the averaged column's type
         * @return {@code DOUBLE} with the argument's nullability, or Calcite's default for a family with
         *         no known mean type
         */
        @Override
        public RelDataType deriveAvgAggType(RelDataTypeFactory typeFactory, RelDataType argumentType) {
            SqlTypeName mean = switch (argumentType.getSqlTypeName()) {
                case TINYINT, SMALLINT, INTEGER, BIGINT, REAL, FLOAT, DOUBLE -> SqlTypeName.DOUBLE;
                default -> null;
            };
            if (mean == null) {
                return super.deriveAvgAggType(typeFactory, argumentType);
            }
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(mean), argumentType.isNullable());
        }
    };

    private DslTypeSystems() {}
}
