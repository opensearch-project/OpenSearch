/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.catalog;

import org.opensearch.cluster.Diff;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.List;

public class CatalogPublishesInProgressTests extends OpenSearchTestCase {

    private static PublishEntry entry(String publishId, String indexName) {
        return PublishEntry.builder()
            .publishId(publishId)
            .indexName(indexName)
            .indexUUID("uuid-" + indexName)
            .startedAt(1L)
            .build();
    }

    public void testEmptyConstant() {
        assertTrue(CatalogPublishesInProgress.EMPTY.isEmpty());
        assertEquals(0, CatalogPublishesInProgress.EMPTY.entries().size());
    }

    public void testEmptyWriteableRoundTrip() throws Exception {
        CatalogPublishesInProgress copy = serde(CatalogPublishesInProgress.EMPTY);
        assertEquals(CatalogPublishesInProgress.EMPTY, copy);
        assertTrue(copy.isEmpty());
    }

    public void testSingleEntryRoundTrip() throws Exception {
        CatalogPublishesInProgress original = new CatalogPublishesInProgress(List.of(entry("p1", "idx1")));
        CatalogPublishesInProgress copy = serde(original);
        assertEquals(original, copy);
        assertEquals(1, copy.entries().size());
        assertEquals("p1", copy.entries().get(0).publishId());
    }

    public void testMultiEntryPreservesOrder() throws Exception {
        CatalogPublishesInProgress original = new CatalogPublishesInProgress(
            Arrays.asList(entry("p1", "idx-a"), entry("p2", "idx-b"), entry("p3", "idx-c"))
        );
        CatalogPublishesInProgress copy = serde(original);
        assertEquals(3, copy.entries().size());
        assertEquals("p1", copy.entries().get(0).publishId());
        assertEquals("p2", copy.entries().get(1).publishId());
        assertEquals("p3", copy.entries().get(2).publishId());
    }

    public void testEntryByPublishId() {
        CatalogPublishesInProgress c = new CatalogPublishesInProgress(List.of(entry("p1", "a"), entry("p2", "b")));
        assertEquals("a", c.entry("p1").indexName());
        assertEquals("b", c.entry("p2").indexName());
        assertNull(c.entry("does-not-exist"));
    }

    public void testEntryForIndex() {
        CatalogPublishesInProgress c = new CatalogPublishesInProgress(List.of(entry("p1", "a"), entry("p2", "b")));
        assertEquals("p1", c.entryForIndex("a").publishId());
        assertEquals("p2", c.entryForIndex("b").publishId());
        assertNull(c.entryForIndex("c"));
    }

    public void testWithAddedEntry() {
        CatalogPublishesInProgress start = CatalogPublishesInProgress.EMPTY;
        CatalogPublishesInProgress after = start.withAddedEntry(entry("p1", "a"));
        assertTrue(start.isEmpty());
        assertEquals(1, after.entries().size());
    }

    public void testWithAddedEntryRejectsDuplicate() {
        CatalogPublishesInProgress c = new CatalogPublishesInProgress(List.of(entry("p1", "a")));
        expectThrows(IllegalStateException.class, () -> c.withAddedEntry(entry("p1", "anything")));
    }

    public void testWithUpdatedEntry() {
        PublishEntry original = entry("p1", "a");
        PublishEntry mutated = original.withPhase(PublishPhase.PUBLISHING);
        CatalogPublishesInProgress c = new CatalogPublishesInProgress(List.of(original));
        CatalogPublishesInProgress after = c.withUpdatedEntry(mutated);
        assertEquals(PublishPhase.INITIALIZED, c.entry("p1").phase());
        assertEquals(PublishPhase.PUBLISHING, after.entry("p1").phase());
    }

    public void testWithUpdatedEntryRejectsMissing() {
        CatalogPublishesInProgress c = CatalogPublishesInProgress.EMPTY;
        expectThrows(IllegalStateException.class, () -> c.withUpdatedEntry(entry("ghost", "a")));
    }

    public void testWithRemovedEntry() {
        CatalogPublishesInProgress c = new CatalogPublishesInProgress(List.of(entry("p1", "a"), entry("p2", "b")));
        CatalogPublishesInProgress after = c.withRemovedEntry("p1");
        assertNull(after.entry("p1"));
        assertNotNull(after.entry("p2"));
        assertEquals(1, after.entries().size());
    }

    public void testWithRemovedEntryMissingIsNoOp() {
        CatalogPublishesInProgress c = new CatalogPublishesInProgress(List.of(entry("p1", "a")));
        CatalogPublishesInProgress after = c.withRemovedEntry("does-not-exist");
        assertSame(c, after);
    }

    public void testDiffRoundTripAdd() throws Exception {
        CatalogPublishesInProgress before = CatalogPublishesInProgress.EMPTY;
        CatalogPublishesInProgress after = before.withAddedEntry(entry("p1", "a"));
        assertDiffRoundTrip(before, after);
    }

    public void testDiffRoundTripUpdate() throws Exception {
        CatalogPublishesInProgress before = new CatalogPublishesInProgress(List.of(entry("p1", "a")));
        CatalogPublishesInProgress after = before.withUpdatedEntry(before.entry("p1").withPhase(PublishPhase.PUBLISHING));
        assertDiffRoundTrip(before, after);
    }

    public void testDiffRoundTripRemove() throws Exception {
        CatalogPublishesInProgress before = new CatalogPublishesInProgress(List.of(entry("p1", "a"), entry("p2", "b")));
        CatalogPublishesInProgress after = before.withRemovedEntry("p1");
        assertDiffRoundTrip(before, after);
    }

    public void testContextIsAllContexts() {
        assertEquals(Metadata.ALL_CONTEXTS, CatalogPublishesInProgress.EMPTY.context());
    }

    public void testType() {
        assertEquals("catalog_publishes", CatalogPublishesInProgress.TYPE);
        assertEquals(CatalogPublishesInProgress.TYPE, CatalogPublishesInProgress.EMPTY.getWriteableName());
    }

    private static CatalogPublishesInProgress serde(CatalogPublishesInProgress original) throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new CatalogPublishesInProgress(in);
            }
        }
    }

    private static void assertDiffRoundTrip(CatalogPublishesInProgress before, CatalogPublishesInProgress after) throws Exception {
        Diff<Metadata.Custom> diff = after.diff(before);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            diff.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                Diff<Metadata.Custom> readDiff = CatalogPublishesInProgress.readDiffFrom(in);
                CatalogPublishesInProgress applied = (CatalogPublishesInProgress) readDiff.apply(before);
                assertEquals(after, applied);
            }
        }
    }
}
