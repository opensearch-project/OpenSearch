/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;
import org.opensearch.wlm.MutableWorkloadGroupFragment.ResiliencyMode;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MutableWorkloadGroupFragmentTests extends OpenSearchTestCase {

    public void testSerializationDeserialization() throws IOException {
        Map<ResourceType, Double> resourceLimits = new HashMap<>();
        resourceLimits.put(ResourceType.CPU, 0.5);
        resourceLimits.put(ResourceType.MEMORY, 0.75);
        MutableWorkloadGroupFragment mutableWorkloadGroupFragment = new MutableWorkloadGroupFragment(
            MutableWorkloadGroupFragment.ResiliencyMode.SOFT,
            resourceLimits
        );
        BytesStreamOutput out = new BytesStreamOutput();
        mutableWorkloadGroupFragment.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        MutableWorkloadGroupFragment deserializedGroup = new MutableWorkloadGroupFragment(in);
        assertEquals(mutableWorkloadGroupFragment, deserializedGroup);
    }

    public void testSerializationDeserializationWithNull() throws IOException {
        MutableWorkloadGroupFragment mutableWorkloadGroupFragment = new MutableWorkloadGroupFragment();
        BytesStreamOutput out = new BytesStreamOutput();
        mutableWorkloadGroupFragment.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        MutableWorkloadGroupFragment deserializedGroup = new MutableWorkloadGroupFragment(in);
        assertEquals(0, deserializedGroup.getResourceLimits().size());
        assertEquals(mutableWorkloadGroupFragment.getResiliencyMode(), deserializedGroup.getResiliencyMode());
    }

    public void testValidateResourceLimits() {
        Map<ResourceType, Double> invalidLimits = new HashMap<>();
        invalidLimits.put(ResourceType.CPU, 1.5);
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            MutableWorkloadGroupFragment.validateResourceLimits(invalidLimits);
        });
        String expectedMessage = "resource value should be greater than 0 and less or equal to 1.0";
        String actualMessage = exception.getMessage();
        assertTrue(actualMessage.contains(expectedMessage));
    }

    public void testSetMethodsWithNullAndEmptyValues() {
        MutableWorkloadGroupFragment workloadGroup = new MutableWorkloadGroupFragment();
        workloadGroup.setResiliencyMode(null);
        assertNull(workloadGroup.getResiliencyMode());
        workloadGroup.setResourceLimits(null);
        assertNull(workloadGroup.getResourceLimits());
        workloadGroup.setResourceLimits(new HashMap<>());
        assertEquals(0, workloadGroup.getResourceLimits().size());
    }

    // The fragment is positional on the wire, so where each bag sits is part of the format: the throttling bag follows
    // settings, the queue bag follows the throttling bag, and nothing follows the queue bag. Each bag is optional, and an
    // absent bag (null: keep the stored one on an update) is distinct from an empty one (clear it). Read back field by field,
    // since a round trip through the same reader cannot see an order change made to both sides.
    public void testThrottlingAndQueueBagsTrailSettingsOnTheWire() throws IOException {
        Settings throttling = Settings.builder().put("node_limit", 7).build();
        Settings queue = Settings.builder().put("size_per_bucket", 3).build();
        MutableWorkloadGroupFragment withBags = new MutableWorkloadGroupFragment(
            ResiliencyMode.ENFORCED,
            Map.of(ResourceType.MEMORY, 0.5),
            Settings.EMPTY,
            throttling,
            queue
        );
        try (StreamInput in = written(withBags, WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION)) {
            assertTrue("resource limits present", in.readBoolean());
            assertEquals(
                Map.of(ResourceType.MEMORY, 0.5),
                in.readMap((i) -> ResourceType.fromName(i.readString()), StreamInput::readDouble)
            );
            assertEquals("enforced", in.readOptionalString());
            assertEquals(Settings.EMPTY, Settings.readOptionalSettingsFromStream(in));
            assertEquals(throttling, Settings.readOptionalSettingsFromStream(in));
            assertEquals(queue, Settings.readOptionalSettingsFromStream(in));
            assertEquals(0, in.available());
        }

        MutableWorkloadGroupFragment emptyThrottlingNoQueue = new MutableWorkloadGroupFragment();
        emptyThrottlingNoQueue.setThrottling(Settings.EMPTY);
        try (StreamInput in = written(emptyThrottlingNoQueue, WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION)) {
            assertFalse("no resource limits", in.readBoolean());
            assertNull(in.readOptionalString());
            assertNull("no settings bag", Settings.readOptionalSettingsFromStream(in));
            assertEquals("an empty throttling bag", Settings.EMPTY, Settings.readOptionalSettingsFromStream(in));
            assertNull("no queue bag", Settings.readOptionalSettingsFromStream(in));
            assertEquals(0, in.available());
        }
    }

    // An older peer reads neither bag, so neither may be written to it: the stream ends where that peer's reader stops.
    public void testBagsAreLeftOffTheWireForAnOlderPeer() throws IOException {
        Version older = VersionUtils.getPreviousVersion(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION);
        MutableWorkloadGroupFragment withBags = new MutableWorkloadGroupFragment(
            ResiliencyMode.ENFORCED,
            Map.of(ResourceType.MEMORY, 0.5),
            Settings.EMPTY,
            Settings.builder().put("node_limit", 7).build(),
            Settings.builder().put("size_per_bucket", 3).build()
        );
        MutableWorkloadGroupFragment withoutBags = new MutableWorkloadGroupFragment(
            ResiliencyMode.ENFORCED,
            Map.of(ResourceType.MEMORY, 0.5),
            Settings.EMPTY
        );
        try (BytesStreamOutput withBagsOut = new BytesStreamOutput(); BytesStreamOutput withoutBagsOut = new BytesStreamOutput()) {
            withBagsOut.setVersion(older);
            withBags.writeTo(withBagsOut);
            withoutBagsOut.setVersion(older);
            withoutBags.writeTo(withoutBagsOut);
            assertEquals("the bytes of the same fragment without bags", withoutBagsOut.bytes(), withBagsOut.bytes());
        }
        try (StreamInput in = written(withBags, older)) {
            MutableWorkloadGroupFragment decoded = new MutableWorkloadGroupFragment(in);
            assertEquals(0, in.available());
            assertNull(decoded.getThrottling());
            assertNull(decoded.getQueue());
        }
    }

    // Workload group equality, and with it cluster-state diffs, must see a change to the queue alone.
    public void testQueueTakesPartInEqualsAndHashCode() {
        MutableWorkloadGroupFragment queued = fragmentWithQueue(Settings.builder().put("size_per_bucket", 3).build());
        MutableWorkloadGroupFragment same = fragmentWithQueue(Settings.builder().put("size_per_bucket", 3).build());
        assertEquals(queued, same);
        assertEquals(queued.hashCode(), same.hashCode());
        for (Settings otherQueue : List.of(Settings.builder().put("size_per_bucket", 4).build(), Settings.EMPTY)) {
            MutableWorkloadGroupFragment other = fragmentWithQueue(otherQueue);
            assertNotEquals(queued, other);
            assertNotEquals(queued.hashCode(), other.hashCode());
        }
    }

    private static MutableWorkloadGroupFragment fragmentWithQueue(Settings queue) {
        return new MutableWorkloadGroupFragment(
            ResiliencyMode.ENFORCED,
            Map.of(ResourceType.MEMORY, 0.5),
            Settings.EMPTY,
            Settings.builder().put("node_limit", 7).build(),
            queue
        );
    }

    private static StreamInput written(MutableWorkloadGroupFragment fragment, Version version) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(version);
        fragment.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        in.setVersion(version);
        return in;
    }
}
