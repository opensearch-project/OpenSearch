/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.ExceptionsHelper;
import org.opensearch.Version;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;
import org.opensearch.wlm.MutableWorkloadGroupFragment;
import org.opensearch.wlm.MutableWorkloadGroupFragment.ResiliencyMode;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadGroupQueueSettings;
import org.opensearch.wlm.WorkloadGroupThrottleSettings;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.plugin.wlm.WorkloadManagementTestUtils.NAME_ONE;
import static org.opensearch.plugin.wlm.WorkloadManagementTestUtils.workloadGroupOne;

public class UpdateWorkloadGroupRequestTests extends OpenSearchTestCase {

    /**
     * Test case to verify the serialization and deserialization of UpdateWorkloadGroupRequest.
     */
    public void testSerialization() throws IOException {
        UpdateWorkloadGroupRequest request = new UpdateWorkloadGroupRequest(NAME_ONE, workloadGroupOne.getMutableWorkloadGroupFragment());
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        UpdateWorkloadGroupRequest otherRequest = new UpdateWorkloadGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
        assertEquals(request.getmMutableWorkloadGroupFragment(), otherRequest.getmMutableWorkloadGroupFragment());
    }

    /**
     * Test case to verify the serialization and deserialization of UpdateWorkloadGroupRequest with only name field.
     */
    public void testSerializationOnlyName() throws IOException {
        UpdateWorkloadGroupRequest request = new UpdateWorkloadGroupRequest(
            NAME_ONE,
            new MutableWorkloadGroupFragment(null, new HashMap<>())
        );
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        UpdateWorkloadGroupRequest otherRequest = new UpdateWorkloadGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
        assertEquals(request.getmMutableWorkloadGroupFragment(), otherRequest.getmMutableWorkloadGroupFragment());
    }

    /**
     * Test case to verify the serialization and deserialization of UpdateWorkloadGroupRequest with only resourceLimits field.
     */
    public void testSerializationOnlyResourceLimit() throws IOException {
        UpdateWorkloadGroupRequest request = new UpdateWorkloadGroupRequest(
            NAME_ONE,
            new MutableWorkloadGroupFragment(null, Map.of(ResourceType.MEMORY, 0.4))
        );
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        UpdateWorkloadGroupRequest otherRequest = new UpdateWorkloadGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
        assertEquals(request.getmMutableWorkloadGroupFragment(), otherRequest.getmMutableWorkloadGroupFragment());
    }

    public void testThrottledUpdateRequiresSupportedWireVersion() throws IOException {
        UpdateWorkloadGroupRequest request = new UpdateWorkloadGroupRequest(
            NAME_ONE,
            new MutableWorkloadGroupFragment(null, Map.of(), Settings.EMPTY, Settings.builder().put("node_limit", 5).build())
        );
        BytesStreamOutput oldOutput = new BytesStreamOutput();
        oldOutput.setVersion(VersionUtils.getPreviousVersion(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION));

        IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> request.writeTo(oldOutput));
        assertTrue(error.getMessage(), error.getMessage().contains(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION.toString()));

        BytesStreamOutput currentOutput = new BytesStreamOutput();
        currentOutput.setVersion(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION);
        request.writeTo(currentOutput);
        StreamInput currentInput = currentOutput.bytes().streamInput();
        currentInput.setVersion(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION);
        UpdateWorkloadGroupRequest restored = new UpdateWorkloadGroupRequest(currentInput);
        assertEquals(
            Integer.valueOf(5),
            WorkloadGroupThrottleSettings.NODE_LIMIT.get(restored.getmMutableWorkloadGroupFragment().getThrottling())
        );

        BytesStreamOutput legacyOutput = new BytesStreamOutput();
        legacyOutput.setVersion(VersionUtils.getPreviousVersion(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION));
        new UpdateWorkloadGroupRequest(NAME_ONE, new MutableWorkloadGroupFragment(null, Map.of())).writeTo(legacyOutput);
    }

    // A queue needs no throttle limit, and an older node would silently drop it, so it is gated like throttling.
    public void testQueueOnlyUpdateRequiresSupportedWireVersion() throws IOException {
        UpdateWorkloadGroupRequest request = new UpdateWorkloadGroupRequest(
            NAME_ONE,
            new MutableWorkloadGroupFragment(
                null,
                Map.of(),
                Settings.EMPTY,
                Settings.EMPTY,
                Settings.builder().put("size_per_bucket", 50).build()
            )
        );
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
        UpdateWorkloadGroupRequest restored = new UpdateWorkloadGroupRequest(currentInput);
        assertEquals(
            Integer.valueOf(50),
            WorkloadGroupQueueSettings.SIZE_PER_BUCKET.get(restored.getmMutableWorkloadGroupFragment().getQueue())
        );
    }

    // Request queueing is not available in this version: an update that sets queue, in any form, is rejected while it is
    // parsed, so the error never depends on what the queue holds.
    public void testFromXContentRejectsAnyQueue() throws IOException {
        for (String body : List.of(
            "{\"queue\":{\"size_per_bucket\":7}}",
            "{\"queue\":{\"timeout\":\"30s\"}}",
            "{\"queue\":{\"size_per_bucket\":-1}}",
            "{\"queue\":{\"size_per_bucket\":null}}",
            "{\"queue\":{}}",
            "{\"queue\":null}",
            "{\"throttling\":{\"node_limit\":5},\"queue\":null}"
        )) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> UpdateWorkloadGroupRequest.fromXContent(createParser(JsonXContent.jsonXContent, body), NAME_ONE)
            );
            assertEquals(body, CreateWorkloadGroupRequest.QUEUEING_NOT_AVAILABLE, e.getMessage());
            assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(e));
        }
        UpdateWorkloadGroupRequest withoutQueue = UpdateWorkloadGroupRequest.fromXContent(
            createParser(JsonXContent.jsonXContent, "{\"throttling\":{\"node_limit\":5}}"),
            NAME_ONE
        );
        assertNull("an update that does not mention the queue keeps it", withoutQueue.getmMutableWorkloadGroupFragment().getQueue());
        assertNull(withoutQueue.validate());
    }

    // The backstop for an update another node forwarded, which may be on a version that accepts a queue: validate() runs
    // on every node the request reaches, the cluster manager included. On the wire an absent queue stays null, while
    // {} and null arrive as an empty bag, so both are seen.
    public void testValidateRejectsAQueueDecodedFromTheWire() throws IOException {
        for (Settings queue : List.of(Settings.builder().put("size_per_bucket", 7).build(), Settings.EMPTY)) {
            UpdateWorkloadGroupRequest decoded = copyOverTheWire(
                new UpdateWorkloadGroupRequest(
                    NAME_ONE,
                    new MutableWorkloadGroupFragment(null, Map.of(), Settings.EMPTY, Settings.EMPTY, queue)
                )
            );
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, decoded::validate);
            assertEquals(queue.toString(), CreateWorkloadGroupRequest.QUEUEING_NOT_AVAILABLE, e.getMessage());
        }
        UpdateWorkloadGroupRequest withoutQueue = UpdateWorkloadGroupRequest.fromXContent(
            createParser(JsonXContent.jsonXContent, "{\"resiliency_mode\":\"soft\"}"),
            NAME_ONE
        );
        UpdateWorkloadGroupRequest decoded = copyOverTheWire(withoutQueue);
        assertNull(decoded.getmMutableWorkloadGroupFragment().getQueue());
        assertNull(decoded.validate());
    }

    // The parent task id and cluster_manager_timeout reach the cluster manager, so the forwarded update is a child of the
    // entry node's task and the cluster manager applies the caller's timeout.
    public void testSuperclassFieldsSurviveTheWire() throws IOException {
        UpdateWorkloadGroupRequest request = new UpdateWorkloadGroupRequest(
            NAME_ONE,
            new MutableWorkloadGroupFragment(ResiliencyMode.SOFT, Map.of(ResourceType.MEMORY, 0.4))
        );
        TaskId parent = new TaskId(randomAlphaOfLength(8), randomNonNegativeLong());
        // Never the default, which a reader that drops the timeout would also produce.
        TimeValue timeout = randomValueOtherThan(
            ClusterManagerNodeRequest.DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT,
            () -> TimeValue.timeValueSeconds(randomIntBetween(1, 120))
        );
        request.setParentTask(parent);
        request.clusterManagerNodeTimeout(timeout);
        for (Version version : List.of(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION, Version.CURRENT)) {
            BytesStreamOutput out = new BytesStreamOutput();
            out.setVersion(version);
            request.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            in.setVersion(version);
            UpdateWorkloadGroupRequest restored = new UpdateWorkloadGroupRequest(in);
            assertEquals(0, in.available());
            assertEquals(parent, restored.getParentTask());
            assertEquals(timeout, restored.clusterManagerNodeTimeout());
            assertEquals(NAME_ONE, restored.getName());
            assertEquals(request.getmMutableWorkloadGroupFragment(), restored.getmMutableWorkloadGroupFragment());
        }
    }

    // From the throttling version the request starts with the superclass fields, in the order ClusterManagerNodeRequest
    // writes them (parent task id, then cluster_manager_timeout), followed by the name and the fragment. Compared byte for
    // byte, since a round trip through the same reader cannot see an order change made to both sides.
    public void testCurrentPeerFormatStartsWithTheSuperclassFields() throws IOException {
        MutableWorkloadGroupFragment fragment = new MutableWorkloadGroupFragment(ResiliencyMode.SOFT, Map.of(ResourceType.MEMORY, 0.4));
        UpdateWorkloadGroupRequest request = new UpdateWorkloadGroupRequest(NAME_ONE, fragment);
        TaskId parent = new TaskId("node", 7);
        request.setParentTask(parent);
        request.clusterManagerNodeTimeout(TimeValue.timeValueSeconds(3));

        BytesStreamOutput expected = new BytesStreamOutput();
        expected.setVersion(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION);
        parent.writeTo(expected);
        expected.writeTimeValue(TimeValue.timeValueSeconds(3));
        expected.writeString(NAME_ONE);
        fragment.writeTo(expected);
        BytesStreamOutput actual = new BytesStreamOutput();
        actual.setVersion(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION);
        request.writeTo(actual);
        assertEquals("parent task id, cluster_manager_timeout, name, fragment", expected.bytes(), actual.bytes());
    }

    // An older peer neither writes nor reads the superclass fields, so its format is unchanged in both directions.
    public void testOlderPeerFormatIsUnchanged() throws IOException {
        Version older = VersionUtils.getPreviousVersion(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION);
        MutableWorkloadGroupFragment fragment = new MutableWorkloadGroupFragment(ResiliencyMode.SOFT, Map.of(ResourceType.MEMORY, 0.4));
        UpdateWorkloadGroupRequest request = new UpdateWorkloadGroupRequest(NAME_ONE, fragment);
        request.setParentTask(new TaskId("node", 7));
        request.clusterManagerNodeTimeout(TimeValue.timeValueSeconds(3));

        BytesStreamOutput expected = new BytesStreamOutput();
        expected.setVersion(older);
        expected.writeString(NAME_ONE);
        fragment.writeTo(expected);
        BytesStreamOutput actual = new BytesStreamOutput();
        actual.setVersion(older);
        request.writeTo(actual);
        assertEquals(expected.bytes(), actual.bytes());

        StreamInput in = expected.bytes().streamInput();
        in.setVersion(older);
        UpdateWorkloadGroupRequest decoded = new UpdateWorkloadGroupRequest(in);
        assertEquals(0, in.available());
        assertEquals(NAME_ONE, decoded.getName());
        assertEquals(ResiliencyMode.SOFT, decoded.getmMutableWorkloadGroupFragment().getResiliencyMode());
        assertEquals(Map.of(ResourceType.MEMORY, 0.4), decoded.getmMutableWorkloadGroupFragment().getResourceLimits());
        assertEquals(TaskId.EMPTY_TASK_ID, decoded.getParentTask());
        assertEquals(ClusterManagerNodeRequest.DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT, decoded.clusterManagerNodeTimeout());
    }

    private static UpdateWorkloadGroupRequest copyOverTheWire(UpdateWorkloadGroupRequest request) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        return new UpdateWorkloadGroupRequest(out.bytes().streamInput());
    }

    /**
     * Tests invalid ResourceType.
     */
    public void testInvalidResourceLimitList() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new UpdateWorkloadGroupRequest(
                NAME_ONE,
                new MutableWorkloadGroupFragment(
                    ResiliencyMode.MONITOR,
                    Map.of(ResourceType.MEMORY, 0.3, ResourceType.fromName("random"), 0.4)
                )
            )
        );
    }

    /**
     * Tests invalid resiliencyMode.
     */
    public void testInvalidEnforcement() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new UpdateWorkloadGroupRequest(
                NAME_ONE,
                new MutableWorkloadGroupFragment(ResiliencyMode.fromName("random"), Map.of(ResourceType.fromName("memory"), 0.3))
            )
        );
    }
}
