/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

public class AllocationIdSplitTests extends OpenSearchTestCase {

    public void testNewSplitCreatesChildAllocationIds() {
        AllocationId original = AllocationId.newInitializing();
        AllocationId split = AllocationId.newSplit(original, 3);

        assertEquals(original.getId(), split.getId());
        assertNull(split.getRelocationId());
        assertNotNull(split.getSplitChildAllocationIds());
        assertEquals(3, split.getSplitChildAllocationIds().size());
        assertNull(split.getParentAllocationId());
    }

    public void testNewSplitChildIdsAreUnique() {
        AllocationId split = AllocationId.newSplit(AllocationId.newInitializing(), 5);
        List<String> childIds = split.getSplitChildAllocationIds();
        assertEquals(5, childIds.stream().distinct().count());
    }

    public void testNewTargetSplitSetsParentAllocationId() {
        AllocationId parent = AllocationId.newSplit(AllocationId.newInitializing(), 2);
        String childAllocId = parent.getSplitChildAllocationIds().get(0);

        AllocationId child = AllocationId.newTargetSplit(parent, childAllocId);

        assertEquals(childAllocId, child.getId());
        assertNull(child.getRelocationId());
        assertNull(child.getSplitChildAllocationIds());
        assertEquals(parent.getId(), child.getParentAllocationId());
    }

    public void testCancelSplitClearsChildIds() {
        AllocationId split = AllocationId.newSplit(AllocationId.newInitializing(), 2);
        AllocationId cancelled = AllocationId.cancelSplit(split);

        assertEquals(split.getId(), cancelled.getId());
        assertNull(cancelled.getSplitChildAllocationIds());
        assertNull(cancelled.getParentAllocationId());
    }

    public void testFinishSplitClearsParentId() {
        AllocationId parent = AllocationId.newSplit(AllocationId.newInitializing(), 2);
        AllocationId child = AllocationId.newTargetSplit(parent, parent.getSplitChildAllocationIds().get(0));
        AllocationId finished = AllocationId.finishSplit(child);

        assertEquals(child.getId(), finished.getId());
        assertNull(finished.getParentAllocationId());
        assertNull(finished.getSplitChildAllocationIds());
    }

    public void testEqualsWithSplitFields() {
        AllocationId a = AllocationId.newSplit(AllocationId.newInitializing(), 2);
        AllocationId b = AllocationId.newSplit(AllocationId.newInitializing(), 2);

        // Different ids, so not equal
        assertNotEquals(a, b);

        // Same object
        assertEquals(a, a);
    }

    public void testEqualsNullSplitFieldsMatchOriginal() {
        AllocationId a = AllocationId.newInitializing();
        AllocationId b = AllocationId.newInitializing();

        // Different random ids
        assertNotEquals(a, b);

        // Initializing with same id
        AllocationId c = AllocationId.newInitializing("fixed-id");
        AllocationId d = AllocationId.newInitializing("fixed-id");
        assertEquals(c, d);
    }

    public void testSerializationRoundTrip() throws IOException {
        AllocationId original = AllocationId.newSplit(AllocationId.newInitializing(), 3);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        AllocationId deserialized = new AllocationId(in);

        assertEquals(original, deserialized);
        assertEquals(original.getSplitChildAllocationIds(), deserialized.getSplitChildAllocationIds());
    }

    public void testSerializationRoundTripWithParentId() throws IOException {
        AllocationId parent = AllocationId.newSplit(AllocationId.newInitializing(), 2);
        AllocationId child = AllocationId.newTargetSplit(parent, parent.getSplitChildAllocationIds().get(0));

        BytesStreamOutput out = new BytesStreamOutput();
        child.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        AllocationId deserialized = new AllocationId(in);

        assertEquals(child, deserialized);
        assertEquals(child.getParentAllocationId(), deserialized.getParentAllocationId());
    }

    public void testSerializationRoundTripNoSplitFields() throws IOException {
        AllocationId original = AllocationId.newInitializing();

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        AllocationId deserialized = new AllocationId(in);

        assertEquals(original, deserialized);
        assertNull(deserialized.getSplitChildAllocationIds());
        assertNull(deserialized.getParentAllocationId());
    }

    public void testRelocationFactoryMethodsPreserveNullSplitFields() {
        AllocationId init = AllocationId.newInitializing();
        AllocationId reloc = AllocationId.newRelocation(init);

        assertNull(reloc.getSplitChildAllocationIds());
        assertNull(reloc.getParentAllocationId());
        assertNotNull(reloc.getRelocationId());

        AllocationId target = AllocationId.newTargetRelocation(reloc);
        assertNull(target.getSplitChildAllocationIds());
        assertNull(target.getParentAllocationId());

        AllocationId cancelled = AllocationId.cancelRelocation(reloc);
        assertNull(cancelled.getSplitChildAllocationIds());

        AllocationId finished = AllocationId.finishRelocation(reloc);
        assertNull(finished.getSplitChildAllocationIds());
    }

    public void testToXContentWithSplitChildIds() throws IOException {
        AllocationId split = AllocationId.newSplit(AllocationId.newInitializing(), 2);
        XContentBuilder builder = JsonXContent.contentBuilder();
        split.toXContent(builder, null);
        String json = builder.toString();
        assertTrue(json.contains("split_child_allocation_ids"));
        assertTrue(json.contains(split.getSplitChildAllocationIds().get(0)));
        assertTrue(json.contains(split.getSplitChildAllocationIds().get(1)));
    }

    public void testToXContentWithParentAllocationId() throws IOException {
        AllocationId parent = AllocationId.newSplit(AllocationId.newInitializing(), 1);
        AllocationId child = AllocationId.newTargetSplit(parent, parent.getSplitChildAllocationIds().get(0));
        XContentBuilder builder = JsonXContent.contentBuilder();
        child.toXContent(builder, null);
        String json = builder.toString();
        assertTrue(json.contains("parent_allocation_id"));
        assertTrue(json.contains(parent.getId()));
    }

    public void testFromXContentRoundTripWithSplitFields() throws IOException {
        AllocationId split = AllocationId.newSplit(AllocationId.newInitializing(), 2);
        XContentBuilder builder = JsonXContent.contentBuilder();
        split.toXContent(builder, null);

        XContentParser parser = createParser(JsonXContent.jsonXContent, builder.toString());
        AllocationId parsed = AllocationId.fromXContent(parser);
        assertEquals(split, parsed);
    }

    public void testHashCodeWithSplitFields() {
        AllocationId split = AllocationId.newSplit(AllocationId.newInitializing(), 2);
        AllocationId cancelled = AllocationId.cancelSplit(split);
        // Different hashcodes because split fields differ
        assertNotEquals(split.hashCode(), cancelled.hashCode());
    }

    public void testToStringWithSplitFields() {
        AllocationId split = AllocationId.newSplit(AllocationId.newInitializing(), 2);
        String str = split.toString();
        assertTrue(str.contains(split.getId()));
        assertTrue(str.contains("splitChildIds"));
    }

    public void testSplitChildAllocationIdsAreUnmodifiable() {
        AllocationId split = AllocationId.newSplit(AllocationId.newInitializing(), 2);
        expectThrows(UnsupportedOperationException.class, () -> split.getSplitChildAllocationIds().add("new-id"));
    }
}
