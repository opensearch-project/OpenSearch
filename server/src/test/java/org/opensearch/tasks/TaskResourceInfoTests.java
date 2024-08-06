/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.tasks.resourcetracker.TaskResourceInfo;
import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;

/**
 * Test cases for TaskResourceInfo
 */
public class TaskResourceInfoTests extends OpenSearchTestCase {
    private final Long cpuUsage = randomNonNegativeLong();
    private final Long memoryUsage = randomNonNegativeLong();
    private final String action = randomAlphaOfLengthBetween(1, 10);
    private final Long taskId = randomNonNegativeLong();
    private final Long parentTaskId = randomNonNegativeLong();
    private final String nodeId = randomAlphaOfLengthBetween(1, 10);
    private TaskResourceInfo taskResourceInfo;
    private TaskResourceUsage taskResourceUsage;

    @Before
    public void setUpVariables() {
        taskResourceUsage = new TaskResourceUsage(cpuUsage, memoryUsage);
        taskResourceInfo = new TaskResourceInfo(action, taskId, parentTaskId, nodeId, taskResourceUsage);
    }

    public void testGetters() {
        assertEquals(action, taskResourceInfo.getAction());
        assertEquals(taskId.longValue(), taskResourceInfo.getTaskId());
        assertEquals(parentTaskId.longValue(), taskResourceInfo.getParentTaskId());
        assertEquals(nodeId, taskResourceInfo.getNodeId());
        assertEquals(taskResourceUsage, taskResourceInfo.getTaskResourceUsage());
    }

    public void testEqualsAndHashCode() {
        TaskResourceInfo taskResourceInfoCopy = new TaskResourceInfo(action, taskId, parentTaskId, nodeId, taskResourceUsage);
        assertEquals(taskResourceInfo, taskResourceInfoCopy);
        assertEquals(taskResourceInfo.hashCode(), taskResourceInfoCopy.hashCode());
        TaskResourceInfo differentTaskResourceInfo = new TaskResourceInfo(
            "differentAction",
            taskId,
            parentTaskId,
            nodeId,
            taskResourceUsage
        );
        assertNotEquals(taskResourceInfo, differentTaskResourceInfo);
        assertNotEquals(taskResourceInfo.hashCode(), differentTaskResourceInfo.hashCode());
    }

    public void testSerialization() throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        taskResourceInfo.writeTo(output);
        StreamInput input = StreamInput.wrap(output.bytes().toBytesRef().bytes);
        TaskResourceInfo deserializedTaskResourceInfo = TaskResourceInfo.readFromStream(input);
        assertEquals(taskResourceInfo, deserializedTaskResourceInfo);
    }

    public void testToString() {
        String expectedString = String.format(
            Locale.ROOT,
            "{\"action\":\"%s\",\"taskId\":%s,\"parentTaskId\":%s,\"nodeId\":\"%s\",\"taskResourceUsage\":{\"cpu_time_in_nanos\":%s,\"memory_in_bytes\":%s}}",
            action,
            taskId,
            parentTaskId,
            nodeId,
            taskResourceUsage.getCpuTimeInNanos(),
            taskResourceUsage.getMemoryInBytes()
        );
        assertTrue(expectedString.equals(taskResourceInfo.toString()));
    }

    public void testToXContent() throws IOException {
        char[] expectedXcontent = String.format(
            Locale.ROOT,
            "{\"action\":\"%s\",\"taskId\":%s,\"parentTaskId\":%s,\"nodeId\":\"%s\",\"taskResourceUsage\":{\"cpu_time_in_nanos\":%s,\"memory_in_bytes\":%s}}",
            action,
            taskId,
            parentTaskId,
            nodeId,
            taskResourceUsage.getCpuTimeInNanos(),
            taskResourceUsage.getMemoryInBytes()
        ).toCharArray();

        XContentBuilder builder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        char[] xContent = BytesReference.bytes(taskResourceInfo.toXContent(builder, ToXContent.EMPTY_PARAMS)).utf8ToString().toCharArray();
        assertEquals(Arrays.hashCode(expectedXcontent), Arrays.hashCode(xContent));
    }
}
