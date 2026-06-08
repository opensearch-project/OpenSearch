/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

import java.math.BigDecimal;

/**
 * Utility for converting Calcite-internal literal values into Java types that the
 * OpenSearch Mapper can parse against a field's declared type.
 *
 * <p>Calcite stores literals in a small set of canonical Java types regardless of
 * the SQL type the literal was written as (see {@code RexLiteral#valueMatchesType}):
 * <ul>
 *   <li>INTEGER / BIGINT / DECIMAL / interval types → {@link BigDecimal}</li>
 *   <li>DOUBLE / FLOAT / REAL → {@link Double}</li>
 *   <li>CHAR / VARCHAR → {@link NlsString} (carries charset + collation)</li>
 *   <li>DATE → {@link DateString}</li>
 *   <li>TIME → {@link TimeString}</li>
 *   <li>TIMESTAMP → {@link TimestampString}</li>
 *   <li>BOOLEAN → {@link Boolean}</li>
 * </ul>
 *
 * <p>Two reasons to convert here:
 * <ol>
 *   <li><b>Wire compatibility.</b> {@code QueryBuilder} subclasses (e.g.
 *       {@link org.opensearch.index.query.TermQueryBuilder}) serialize the value via
 *       {@code StreamOutput.writeGenericValue}, which has a fixed allowlist of types
 *       (Long/Integer/Double/Float/String/Boolean/byte[]/...). It rejects
 *       {@link BigDecimal}, {@link NlsString}, etc. with
 *       {@code "can not write type [class ...]"}.</li>
 *   <li><b>Mapper parsing.</b> {@code FieldMapper#parse} on the data node accepts
 *       {@link Number} subclasses but Calcite's String types like {@link NlsString}
 *       aren't handled — the mapper expects {@link String}.</li>
 * </ol>
 *
 * <p>Numeric narrowing for {@link BigDecimal}: integer-shaped values become
 * {@link Integer} (within {@code int} range), {@link Long} (within {@code long}
 * range), or {@link Double} (otherwise). Fractional values become {@link Double}.
 * Calcite stores TINYINT/SMALLINT/INTEGER/BIGINT/DECIMAL/intervals all as
 * {@link BigDecimal} (see {@code RexLiteral.valueMatchesType}); narrowing is
 * what makes them wire-writable.
 *
 * <p>NOTE: temp workaround — date/time conversion currently uses ISO-8601 strings,
 * which most OpenSearch date mappers accept by default but isn't guaranteed for
 * custom formats. Cleaner long-term shape is to consult the column's
 * {@link org.opensearch.analytics.spi.FieldStorageInfo} to dispatch by declared
 * field type, not just by literal type. Needs revisiting.
 *
 * @opensearch.internal
 */
public final class CalciteToOSMapperConversionUtils {

    private CalciteToOSMapperConversionUtils() {}

    /**
     * Convert a {@link RexLiteral}'s underlying value into a Java type the OpenSearch
     * Mapper accepts. Returns {@code null} for SQL NULL literals.
     */
    public static Object literalToOpenSearchValue(RexLiteral literal) {
        if (literal.isNull()) {
            return null;
        }
        return switch (literal.getType().getSqlTypeName()) {
            case BOOLEAN -> literal.getValueAs(Boolean.class);
            case TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL -> narrowBigDecimal(literal.getValueAs(BigDecimal.class));
            case FLOAT, REAL, DOUBLE -> literal.getValueAs(Double.class);
            case CHAR, VARCHAR -> {
                NlsString nls = literal.getValueAs(NlsString.class);
                yield nls == null ? null : nls.getValue();
            }
            case DATE -> {
                DateString date = literal.getValueAs(DateString.class);
                yield date == null ? null : date.toString();
            }
            case TIME, TIME_WITH_LOCAL_TIME_ZONE -> {
                TimeString time = literal.getValueAs(TimeString.class);
                yield time == null ? null : time.toString();
            }
            case TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> {
                TimestampString ts = literal.getValueAs(TimestampString.class);
                yield ts == null ? null : ts.toString();
            }
            // Fallback: stringify whatever Calcite stored. Better than passing through a
            // type the mapper can't parse; surfaces as a "can't parse value" error rather
            // than a type-cast NPE downstream.
            default -> {
                Object raw = literal.getValueAs(Object.class);
                yield raw == null ? null : raw.toString();
            }
        };
    }

    /**
     * Narrow a {@link BigDecimal} to the smallest exact Java numeric type:
     * <ul>
     *   <li>integer-shaped value within {@code int} range → {@link Integer}</li>
     *   <li>integer-shaped value within {@code long} range → {@link Long}</li>
     *   <li>otherwise → {@link Double}</li>
     * </ul>
     * Calcite normalizes {@code 200} (an INTEGER literal) into {@code BigDecimal(200)}
     * with scale 0; OpenSearch mappers parse {@link Integer}/{@link Long} natively and
     * fall over on raw BigDecimal.
     */
    private static Number narrowBigDecimal(BigDecimal value) {
        if (value == null) {
            return null;
        }
        if (value.scale() <= 0 || value.stripTrailingZeros().scale() <= 0) {
            try {
                return value.intValueExact();
            } catch (ArithmeticException notInIntRange) {
                try {
                    return value.longValueExact();
                } catch (ArithmeticException notInLongRange) {
                    // Falls through to Double — out of long range.
                }
            }
        }
        return value.doubleValue();
    }

}
