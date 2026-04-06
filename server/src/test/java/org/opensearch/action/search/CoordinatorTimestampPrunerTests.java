/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class CoordinatorTimestampPrunerTests extends OpenSearchTestCase {

    public void testExtractFromSimpleRange() {
        RangeQueryBuilder range = new RangeQueryBuilder("@timestamp").gte("2026-01-01").lte("2026-01-31");
        CoordinatorTimestampPruner.TimestampBounds bounds = CoordinatorTimestampPruner.extractTimestampBounds(range);
        assertNotNull(bounds);
        assertEquals("2026-01-01", bounds.from.toString());
        assertEquals("2026-01-31", bounds.to.toString());
    }

    public void testExtractFromBoolFilter() {
        BoolQueryBuilder bool = new BoolQueryBuilder()
            .filter(new RangeQueryBuilder("@timestamp").gte("2026-01-01").lte("2026-01-31"))
            .filter(new MatchAllQueryBuilder());
        CoordinatorTimestampPruner.TimestampBounds bounds = CoordinatorTimestampPruner.extractTimestampBounds(bool);
        assertNotNull(bounds);
        assertEquals("2026-01-01", bounds.from.toString());
        assertEquals("2026-01-31", bounds.to.toString());
    }

    public void testExtractFromBoolMust() {
        BoolQueryBuilder bool = new BoolQueryBuilder()
            .must(new RangeQueryBuilder("@timestamp").gte("2026-01-01").lte("2026-01-31"));
        CoordinatorTimestampPruner.TimestampBounds bounds = CoordinatorTimestampPruner.extractTimestampBounds(bool);
        assertNotNull(bounds);
    }

    public void testExtractIgnoresNonTimestampRange() {
        RangeQueryBuilder range = new RangeQueryBuilder("other_field").gte(0).lte(100);
        CoordinatorTimestampPruner.TimestampBounds bounds = CoordinatorTimestampPruner.extractTimestampBounds(range);
        assertNull(bounds);
    }

    public void testExtractFromMatchAll() {
        CoordinatorTimestampPruner.TimestampBounds bounds = CoordinatorTimestampPruner.extractTimestampBounds(
            new MatchAllQueryBuilder()
        );
        assertNull(bounds);
    }

    public void testExtractFromNestedBool() {
        BoolQueryBuilder inner = new BoolQueryBuilder()
            .filter(new RangeQueryBuilder("@timestamp").gte("2026-01-01"));
        BoolQueryBuilder outer = new BoolQueryBuilder().must(inner);
        CoordinatorTimestampPruner.TimestampBounds bounds = CoordinatorTimestampPruner.extractTimestampBounds(outer);
        assertNotNull(bounds);
        assertEquals("2026-01-01", bounds.from.toString());
        assertNull(bounds.to);
    }

    public void testResolveEpochMillis() {
        long result = CoordinatorTimestampPruner.resolveToEpochMillis(1000L, null, System::currentTimeMillis, false);
        assertEquals(1000L, result);
    }

    public void testResolveNull() {
        long resultMin = CoordinatorTimestampPruner.resolveToEpochMillis(null, null, System::currentTimeMillis, false);
        assertEquals(Long.MIN_VALUE, resultMin);

        long resultMax = CoordinatorTimestampPruner.resolveToEpochMillis(null, null, System::currentTimeMillis, true);
        assertEquals(Long.MAX_VALUE, resultMax);
    }

    public void testResolveDateString() {
        long result = CoordinatorTimestampPruner.resolveToEpochMillis(
            "2026-01-01T00:00:00.000Z",
            null,
            System::currentTimeMillis,
            false
        );
        assertTrue(result > 0);
        // 2026-01-01 epoch millis = 1767225600000
        assertEquals(1767225600000L, result);
    }

    public void testResolveWithFormat() {
        long result = CoordinatorTimestampPruner.resolveToEpochMillis(
            "2026-01-01",
            "yyyy-MM-dd",
            System::currentTimeMillis,
            false
        );
        assertEquals(1767225600000L, result);
    }

    public void testExtractFromBoolShould() {
        // should clauses are not extracted because they are OR conditions
        BoolQueryBuilder bool = new BoolQueryBuilder()
            .should(new RangeQueryBuilder("@timestamp").gte("2026-01-01").lte("2026-01-31"));
        CoordinatorTimestampPruner.TimestampBounds bounds = CoordinatorTimestampPruner.extractTimestampBounds(bool);
        assertNull(bounds);
    }

    public void testExtractWithOnlyFrom() {
        RangeQueryBuilder range = new RangeQueryBuilder("@timestamp").gte("2026-01-01");
        CoordinatorTimestampPruner.TimestampBounds bounds = CoordinatorTimestampPruner.extractTimestampBounds(range);
        assertNotNull(bounds);
        assertEquals("2026-01-01", bounds.from.toString());
        assertNull(bounds.to);
    }

    public void testExtractWithOnlyTo() {
        RangeQueryBuilder range = new RangeQueryBuilder("@timestamp").lte("2026-01-31");
        CoordinatorTimestampPruner.TimestampBounds bounds = CoordinatorTimestampPruner.extractTimestampBounds(range);
        assertNotNull(bounds);
        assertNull(bounds.from);
        assertEquals("2026-01-31", bounds.to.toString());
    }
}
