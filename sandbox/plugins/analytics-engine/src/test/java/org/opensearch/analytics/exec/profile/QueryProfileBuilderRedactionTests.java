/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.profile;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link QueryProfileBuilder#redactPhysicalPlan}, which strips storage
 * locations (local filesystem paths or object-store URIs/keys) from DataFusion physical
 * plan text before it's surfaced in the {@code profile=true} API response.
 */
public class QueryProfileBuilderRedactionTests extends OpenSearchTestCase {

    public void testRedactsSingleGroupLocalPath() {
        String plan = "DataSourceExec: file_groups={1 group: "
            + "[[home/ubuntu/data/indices/uuid/0/parquet/_parquet_file_generation_1.parquet]]}, "
            + "projection=[__row_id__@9 + row_base@11 as __row_id__, str0, num1], file_type=parquet";

        String redacted = QueryProfileBuilder.redactPhysicalPlan(plan);

        assertFalse("path must not survive redaction", redacted.contains("parquet_file_generation"));
        assertFalse("path must not survive redaction", redacted.contains("/home/ubuntu"));
        assertTrue("group count must be preserved", redacted.contains("file_groups={1 group: <redacted>}"));
        // Everything outside file_groups must be untouched
        assertTrue(redacted.contains("projection=[__row_id__@9 + row_base@11 as __row_id__, str0, num1]"));
        assertTrue(redacted.contains("file_type=parquet"));
    }

    public void testRedactsMultipleGroups() {
        String plan = "DataSourceExec: file_groups={2 groups: "
            + "[[/data/a.parquet, /data/b.parquet], [/data/c.parquet]]}, file_type=parquet";

        String redacted = QueryProfileBuilder.redactPhysicalPlan(plan);

        assertFalse(redacted.contains("/data/"));
        assertTrue("plural group count must be preserved", redacted.contains("file_groups={2 groups: <redacted>}"));
    }

    public void testRedactsObjectStoreUri() {
        // Object-store backed indices (S3/GCS/Azure) would leak bucket names/keys here instead
        // of local paths — the pattern must not special-case "looks like a filesystem path."
        String plan = "DataSourceExec: file_groups={1 group: [[s3://my-bucket/indices/uuid/0/segment.parquet]]}, " + "file_type=parquet";

        String redacted = QueryProfileBuilder.redactPhysicalPlan(plan);

        assertFalse(redacted.contains("my-bucket"));
        assertFalse(redacted.contains("s3://"));
        assertTrue(redacted.contains("file_groups={1 group: <redacted>}"));
    }

    public void testPlanWithoutFileGroupsIsUnchanged() {
        // Non-scan operators (aggregates, sorts, projections) carry no storage location and
        // must pass through byte-for-byte — the regex must not be overly aggressive.
        String plan = "AggregateExec: mode=Final, gby=[], aggr=[avg(score)]\n" + "  SortExec: TopK(fetch=5), expr=[num0@0 ASC]";

        assertEquals(plan, QueryProfileBuilder.redactPhysicalPlan(plan));
    }

    public void testNullInputReturnsNull() {
        assertNull(QueryProfileBuilder.redactPhysicalPlan(null));
    }

    public void testMultiOperatorPlanOnlyRedactsFileGroupsSegment() {
        // Mirrors a real multi-node plan: only the DataSourceExec's file_groups segment should
        // change; the SortExec line and every other field on the DataSourceExec line survive.
        String plan = "SortPreservingMergeExec: [num0@0 ASC]\n"
            + "  DataSourceExec: file_groups={1 group: [[/secret/path/segment.parquet]]}, "
            + "projection=[num0, str0], predicate=num0 IS NOT NULL, file_type=parquet";

        String redacted = QueryProfileBuilder.redactPhysicalPlan(plan);

        assertTrue(redacted.contains("SortPreservingMergeExec: [num0@0 ASC]"));
        assertTrue(redacted.contains("projection=[num0, str0]"));
        assertTrue(redacted.contains("predicate=num0 IS NOT NULL"));
        assertTrue(redacted.contains("file_type=parquet"));
        assertFalse(redacted.contains("/secret/path"));
        assertTrue(redacted.contains("file_groups={1 group: <redacted>}"));
    }
}
