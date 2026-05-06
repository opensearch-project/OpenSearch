/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

/**
 * End-to-end smoke tests for scalar date/time functions routed through PPL → Calcite →
 * Substrait → DataFusion. Bank fixture row 1: created_at='2024-06-15T10:30:00Z'.
 *
 * <p>Two representative cases:
 * <ul>
 *   <li>{@link #testYear()} — YAML alias with literal-arg injection
 *       ({@code YEAR(ts) → date_part('year', ts)}).</li>
 *   <li>{@link #testConvertTz()} — custom Rust UDF registered with DataFusion
 *       ({@code convert_tz(ts, from_tz, to_tz)}).</li>
 * </ul>
 */
public class ScalarDateTimeFunctionIT extends BaseScalarFunctionIT {

    public void testYear() {
        Object cell = evalScalar("year(created_at)");
        assertNotNull("year() must not be null", cell);
        assertEquals(2024L, ((Number) cell).longValue());
    }

    public void testConvertTz() {
        // row 1: created_at = 2024-06-15T10:30:00Z (UTC).
        // Shifted UTC → +10:00 = 2024-06-15T20:30:00Z, unix seconds = 1718483400.
        // ConvertTzAdapter rewrites PPL's bespoke CONVERT_TZ to our locally-declared
        // SqlFunction("convert_tz") whose Sig is in ADDITIONAL_SCALAR_SIGS;
        // UnixTimestampAdapter does the same to to_unixtime. Isthmus resolves both,
        // DataFusion runs convert_tz via the Rust UDF and to_unixtime natively.
        Object cell = evalScalar("unix_timestamp(convert_tz(created_at, '+00:00', '+10:00'))");
        assertNotNull("convert_tz must not be null", cell);
        assertEquals(1718483400L, ((Number) cell).longValue());
    }
}
