/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.task.commons.task;

import org.opensearch.task.commons.mocks.MockTaskParams;
import org.opensearch.task.commons.worker.WorkerNode;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Test for {@link Task}
 */
public class TaskTests extends OpenSearchTestCase {

    public void testTaskConstructorAndGetters() {
        TaskId taskId = new TaskId("123");
        TaskStatus taskStatus = TaskStatus.UNASSIGNED;
        TaskParams params = new MockTaskParams("mock");
        TaskType taskType = TaskType.MERGE;
        long createdAt = System.currentTimeMillis();
        long assignedAt = createdAt + 1000;
        long startedAt = createdAt + 2000;
        long completedAt = createdAt + 3000;
        long lastHeartbeatAt = createdAt + 2500;
        WorkerNode assignedNode = WorkerNode.createWorkerNode("node1", "nodeip", "nodename");

        Task task = new Task(
            taskId,
            taskStatus,
            params,
            taskType,
            createdAt,
            assignedAt,
            startedAt,
            completedAt,
            lastHeartbeatAt,
            assignedNode
        );

        assertEquals(taskId, task.getTaskId());
        assertEquals(taskStatus, task.getTaskStatus());
        assertEquals(params, task.getParams());
        assertEquals(taskType, task.getTaskType());
        assertEquals(createdAt, task.getCreatedAt());
        assertEquals(assignedAt, task.getAssignedAt());
        assertEquals(startedAt, task.getStartedAt());
        assertEquals(completedAt, task.getCompletedAt());
        assertEquals(lastHeartbeatAt, task.getLastHeartbeatAt());
        assertEquals(assignedNode, task.getAssignedNode());
    }

    public void testBuilderFromTask() {
        TaskId taskId = new TaskId("123");
        TaskStatus taskStatus = TaskStatus.UNASSIGNED;
        TaskParams params = new MockTaskParams("mock");
        TaskType taskType = TaskType.MERGE;
        long createdAt = System.currentTimeMillis();
        long assignedAt = createdAt + 1000;
        long startedAt = createdAt + 2000;
        long completedAt = createdAt + 3000;
        long lastHeartbeatAt = createdAt + 2500;
        WorkerNode assignedNode = WorkerNode.createWorkerNode("node1", "nodeip", "nodename");

        Task originalTask = new Task(
            taskId,
            taskStatus,
            params,
            taskType,
            createdAt,
            assignedAt,
            startedAt,
            completedAt,
            lastHeartbeatAt,
            assignedNode
        );

        Task.Builder builder = Task.Builder.builder(originalTask);
        Task newTask = builder.build();

        assertEquals(originalTask.getTaskId(), newTask.getTaskId());
        assertEquals(originalTask.getTaskStatus(), newTask.getTaskStatus());
        assertEquals(originalTask.getParams(), newTask.getParams());
        assertEquals(originalTask.getTaskType(), newTask.getTaskType());
        assertEquals(originalTask.getCreatedAt(), newTask.getCreatedAt());
        assertEquals(originalTask.getAssignedAt(), newTask.getAssignedAt());
        assertEquals(originalTask.getStartedAt(), newTask.getStartedAt());
        assertEquals(originalTask.getCompletedAt(), newTask.getCompletedAt());
        assertEquals(originalTask.getLastHeartbeatAt(), newTask.getLastHeartbeatAt());
        assertEquals(originalTask.getAssignedNode(), newTask.getAssignedNode());
    }

    public void testBuilderFromAttributes() {
        TaskId taskId = new TaskId("123");
        TaskStatus taskStatus = TaskStatus.UNASSIGNED;
        TaskParams params = new MockTaskParams("mock");
        TaskType taskType = TaskType.MERGE;
        long createdAt = System.currentTimeMillis();

        Task.Builder builder = Task.Builder.builder(taskId, taskStatus, params, taskType, createdAt);
        builder.assignedAt(createdAt + 1000);
        builder.startedAt(createdAt + 2000);
        builder.completedAt(createdAt + 3000);
        builder.lastHeartbeatAt(createdAt + 2500);
        builder.assignedNode(WorkerNode.createWorkerNode("node1", "nodeip", "nodename"));

        Task task = builder.build();

        assertEquals(taskId, task.getTaskId());
        assertEquals(taskStatus, task.getTaskStatus());
        assertEquals(params, task.getParams());
        assertEquals(taskType, task.getTaskType());
        assertEquals(createdAt, task.getCreatedAt());
        assertEquals(createdAt + 1000, task.getAssignedAt());
        assertEquals(createdAt + 2000, task.getStartedAt());
        assertEquals(createdAt + 3000, task.getCompletedAt());
        assertEquals(createdAt + 2500, task.getLastHeartbeatAt());
        assertEquals(WorkerNode.createWorkerNode("node1", "nodeip", "nodename"), task.getAssignedNode());
    }

    public void testBuilderWithNullOptionalFields() {
        TaskId taskId = new TaskId("123");
        TaskStatus taskStatus = TaskStatus.UNASSIGNED;
        TaskParams params = new MockTaskParams("mock");
        TaskType taskType = TaskType.MERGE;
        long createdAt = System.currentTimeMillis();

        Task.Builder builder = Task.Builder.builder(taskId, taskStatus, params, taskType, createdAt);
        Task task = builder.build();

        assertEquals(taskId, task.getTaskId());
        assertEquals(taskStatus, task.getTaskStatus());
        assertEquals(params, task.getParams());
        assertEquals(taskType, task.getTaskType());
        assertEquals(createdAt, task.getCreatedAt());
        assertEquals(0, task.getAssignedAt());
        assertEquals(0, task.getStartedAt());
        assertEquals(0, task.getCompletedAt());
        assertEquals(0, task.getLastHeartbeatAt());
        assertNull(task.getAssignedNode());
    }
}
