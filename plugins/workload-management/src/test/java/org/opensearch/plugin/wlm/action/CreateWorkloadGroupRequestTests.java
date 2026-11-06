/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.ExceptionsHelper;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;
import org.opensearch.wlm.MutableWorkloadGroupFragment;
import org.opensearch.wlm.MutableWorkloadGroupFragment.ResiliencyMode;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadGroupQueueSettings;
import org.opensearch.wlm.WorkloadGroupThrottleSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.plugin.wlm.WorkloadManagementTestUtils.assertEqualWorkloadGroups;
import static org.opensearch.plugin.wlm.WorkloadManagementTestUtils.workloadGroupOne;

public class CreateWorkloadGroupRequestTests extends OpenSearchTestCase {

    /**
     * Test case to verify the serialization and deserialization of CreateWorkloadGroupRequest.
     */
    public void testSerialization() throws IOException {
        CreateWorkloadGroupRequest request = new CreateWorkloadGroupRequest(workloadGroupOne);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        CreateWorkloadGroupRequest otherRequest = new CreateWorkloadGroupRequest(streamInput);
        List<WorkloadGroup> list1 = new ArrayList<>();
        List<WorkloadGroup> list2 = new ArrayList<>();
        list1.add(workloadGroupOne);
        list2.add(otherRequest.getWorkloadGroup());
        assertEqualWorkloadGroups(list1, list2, false);
    }

    public void testThrottledCreateRequiresSupportedWireVersion() throws IOException {
        WorkloadGroup throttledGroup = WorkloadGroup.builder()
            .name("throttled_group")
            ._id("throttled_group_id")
            .mutableWorkloadGroupFragment(
                new MutableWorkloadGroupFragment(
                    ResiliencyMode.ENFORCED,
                    Map.of(ResourceType.MEMORY, 0.5),
                    Settings.EMPTY,
                    Settings.builder().put("node_limit", 5).build()
                )
            )
            .updatedAt(1690934400000L)
            .build();
        CreateWorkloadGroupRequest request = new CreateWorkloadGroupRequest(throttledGroup);
        BytesStreamOutput oldOutput = new BytesStreamOutput();
        oldOutput.setVersion(VersionUtils.getPreviousVersion(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION));

        IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> request.writeTo(oldOutput));
        assertTrue(error.getMessage(), error.getMessage().contains(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION.toString()));

        BytesStreamOutput currentOutput = new BytesStreamOutput();
        currentOutput.setVersion(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION);
        request.writeTo(currentOutput);
        StreamInput currentInput = currentOutput.bytes().streamInput();
        currentInput.setVersion(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION);
        CreateWorkloadGroupRequest restored = new CreateWorkloadGroupRequest(currentInput);
        assertEquals(
            Integer.valueOf(5),
            WorkloadGroupThrottleSettings.NODE_LIMIT.get(restored.getWorkloadGroup().getMutableWorkloadGroupFragment().getThrottling())
        );

        BytesStreamOutput legacyOutput = new BytesStreamOutput();
        legacyOutput.setVersion(VersionUtils.getPreviousVersion(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION));
        new CreateWorkloadGroupRequest(workloadGroupOne).writeTo(legacyOutput);
    }

    // A queue needs no throttle limit, and an older node would silently drop it, so it is gated like throttling.
    public void testQueueOnlyCreateRequiresSupportedWireVersion() throws IOException {
        WorkloadGroup queuedGroup = WorkloadGroup.builder()
            .name("queued_group")
            ._id("queued_group_id")
            .mutableWorkloadGroupFragment(
                new MutableWorkloadGroupFragment(
                    ResiliencyMode.ENFORCED,
                    Map.of(ResourceType.MEMORY, 0.5),
                    Settings.EMPTY,
                    Settings.EMPTY,
                    Settings.builder().put("size_per_bucket", 50).build()
                )
            )
            .updatedAt(1690934400000L)
            .build();
        CreateWorkloadGroupRequest request = new CreateWorkloadGroupRequest(queuedGroup);
        BytesStreamOutput oldOutput = new BytesStreamOutput();
        oldOutput.setVersion(VersionUtils.getPreviousVersion(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION));

        IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> request.writeTo(oldOutput));
        assertTrue(error.getMessage(), error.getMessage().contains("queue"));
        assertTrue(error.getMessage(), error.getMessage().contains(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION.toString()));

        BytesStreamOutput currentOutput = new BytesStreamOutput();
        currentOutput.setVersion(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION);
        request.writeTo(currentOutput);
        StreamInput currentInput = currentOutput.bytes().streamInput();
        currentInput.setVersion(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION);
        CreateWorkloadGroupRequest restored = new CreateWorkloadGroupRequest(currentInput);
        assertEquals(
            Integer.valueOf(50),
            WorkloadGroupQueueSettings.SIZE_PER_BUCKET.get(restored.getWorkloadGroup().getMutableWorkloadGroupFragment().getQueue())
        );
    }

    // Request queueing is not available in this version: a create that sets queue, in any form, is rejected while it is
    // parsed, before the queue could be validated, so the error never depends on what the queue holds.
    public void testFromXContentRejectsAnyQueue() throws IOException {
        for (String queue : List.of("{\"size_per_bucket\":50}", "{\"timeout\":\"30s\"}", "{\"size_per_bucket\":-1}", "{}", "null")) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> CreateWorkloadGroupRequest.fromXContent(createParser(JsonXContent.jsonXContent, createBody(",\"queue\":" + queue)))
            );
            assertEquals(queue, CreateWorkloadGroupRequest.QUEUEING_NOT_AVAILABLE, e.getMessage());
            assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(e));
        }
        CreateWorkloadGroupRequest withoutQueue = CreateWorkloadGroupRequest.fromXContent(
            createParser(JsonXContent.jsonXContent, createBody(""))
        );
        assertEquals("throttled_group", withoutQueue.getWorkloadGroup().getName());
        assertTrue(withoutQueue.getWorkloadGroup().getMutableWorkloadGroupFragment().getQueue().isEmpty());
        assertNull(withoutQueue.validate());
    }

    // The backstop for a create another node forwarded, which may be on a version that accepts a queue: validate() runs on
    // every node the request reaches, the cluster manager included.
    public void testValidateRejectsAQueueDecodedFromTheWire() throws IOException {
        WorkloadGroup queuedGroup = WorkloadGroup.builder()
            .name("queued_group")
            ._id("queued_group_id")
            .mutableWorkloadGroupFragment(
                new MutableWorkloadGroupFragment(
                    ResiliencyMode.ENFORCED,
                    Map.of(ResourceType.MEMORY, 0.5),
                    Settings.EMPTY,
                    Settings.builder().put("node_limit", 5).build(),
                    Settings.builder().put("size_per_bucket", 50).build()
                )
            )
            .updatedAt(1690934400000L)
            .build();
        CreateWorkloadGroupRequest decoded = copyOverTheWire(new CreateWorkloadGroupRequest(queuedGroup));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, decoded::validate);
        assertEquals(CreateWorkloadGroupRequest.QUEUEING_NOT_AVAILABLE, e.getMessage());

        assertNull(copyOverTheWire(new CreateWorkloadGroupRequest(workloadGroupOne)).validate());
    }

    private static CreateWorkloadGroupRequest copyOverTheWire(CreateWorkloadGroupRequest request) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        return new CreateWorkloadGroupRequest(out.bytes().streamInput());
    }

    private static String createBody(String extraFields) {
        return "{\"name\":\"throttled_group\",\"resiliency_mode\":\"enforced\",\"resource_limits\":{\"memory\":0.5},"
            + "\"throttling\":{\"node_limit\":5}"
            + extraFields
            + "}";
    }
}
