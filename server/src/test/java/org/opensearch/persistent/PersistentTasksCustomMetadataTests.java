/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.persistent;

import org.opensearch.ResourceNotFoundException;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.NamedDiff;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.Metadata.Custom;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.UUIDs;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry.Entry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.persistent.PersistentTasksCustomMetadata.Assignment;
import org.opensearch.persistent.PersistentTasksCustomMetadata.Builder;
import org.opensearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.opensearch.persistent.TestPersistentTasksPlugin.State;
import org.opensearch.persistent.TestPersistentTasksPlugin.TestParams;
import org.opensearch.persistent.TestPersistentTasksPlugin.TestPersistentTasksExecutor;
import org.opensearch.test.AbstractDiffableSerializationTestCase;
import org.opensearch.test.VersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

import static org.opensearch.cluster.metadata.Metadata.CONTEXT_MODE_GATEWAY;
import static org.opensearch.cluster.metadata.Metadata.CONTEXT_MODE_SNAPSHOT;
import static org.opensearch.persistent.PersistentTasksExecutor.NO_NODE_FOUND;
import static org.opensearch.test.VersionUtils.allReleasedVersions;
import static org.opensearch.test.VersionUtils.compatibleFutureVersion;
import static org.opensearch.test.VersionUtils.randomVersionBetween;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class PersistentTasksCustomMetadataTests extends AbstractDiffableSerializationTestCase<Custom> {

    @Override
    protected PersistentTasksCustomMetadata createTestInstance() {
        int numberOfTasks = randomInt(10);
        PersistentTasksCustomMetadata.Builder tasks = PersistentTasksCustomMetadata.builder();
        for (int i = 0; i < numberOfTasks; i++) {
            String taskId = UUIDs.base64UUID();
            tasks.addTask(taskId, TestPersistentTasksExecutor.NAME, new TestParams(randomAlphaOfLength(10)), randomAssignment());
            if (randomBoolean()) {
                // From time to time update status
                tasks.updateTaskState(taskId, new State(randomAlphaOfLength(10)));
            }
        }
        return tasks.build();
    }

    @Override
    protected Writeable.Reader<Custom> instanceReader() {
        return PersistentTasksCustomMetadata::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            Arrays.asList(
                new Entry(Metadata.Custom.class, PersistentTasksCustomMetadata.TYPE, PersistentTasksCustomMetadata::new),
                new Entry(NamedDiff.class, PersistentTasksCustomMetadata.TYPE, PersistentTasksCustomMetadata::readDiffFrom),
                new Entry(PersistentTaskParams.class, TestPersistentTasksExecutor.NAME, TestParams::new),
                new Entry(PersistentTaskState.class, TestPersistentTasksExecutor.NAME, State::new)
            )
        );
    }

    @Override
    protected Custom makeTestChanges(Custom testInstance) {
        Builder builder = PersistentTasksCustomMetadata.builder((PersistentTasksCustomMetadata) testInstance);
        switch (randomInt(3)) {
            case 0:
                addRandomTask(builder);
                break;
            case 1:
                if (builder.getCurrentTaskIds().isEmpty()) {
                    addRandomTask(builder);
                } else {
                    builder.reassignTask(pickRandomTask(builder), randomAssignment());
                }
                break;
            case 2:
                if (builder.getCurrentTaskIds().isEmpty()) {
                    addRandomTask(builder);
                } else {
                    builder.updateTaskState(pickRandomTask(builder), randomBoolean() ? new State(randomAlphaOfLength(10)) : null);
                }
                break;
            case 3:
                if (builder.getCurrentTaskIds().isEmpty()) {
                    addRandomTask(builder);
                } else {
                    builder.removeTask(pickRandomTask(builder));
                }
                break;
        }
        return builder.build();
    }

    @Override
    protected Writeable.Reader<Diff<Custom>> diffReader() {
        return PersistentTasksCustomMetadata::readDiffFrom;
    }

    @Override
    protected PersistentTasksCustomMetadata doParseInstance(XContentParser parser) {
        return PersistentTasksCustomMetadata.fromXContent(parser);
    }

    private String addRandomTask(Builder builder) {
        String taskId = UUIDs.base64UUID();
        builder.addTask(taskId, TestPersistentTasksExecutor.NAME, new TestParams(randomAlphaOfLength(10)), randomAssignment());
        return taskId;
    }

    private String pickRandomTask(PersistentTasksCustomMetadata.Builder testInstance) {
        return randomFrom(new ArrayList<>(testInstance.getCurrentTaskIds()));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(
            Arrays.asList(
                new NamedXContentRegistry.Entry(
                    PersistentTaskParams.class,
                    new ParseField(TestPersistentTasksExecutor.NAME),
                    TestParams::fromXContent
                ),
                new NamedXContentRegistry.Entry(
                    PersistentTaskState.class,
                    new ParseField(TestPersistentTasksExecutor.NAME),
                    State::fromXContent
                )
            )
        );
    }

    @SuppressWarnings("unchecked")
    public void testSerializationContext() throws Exception {
        PersistentTasksCustomMetadata testInstance = createTestInstance();
        for (int i = 0; i < randomInt(10); i++) {
            testInstance = (PersistentTasksCustomMetadata) makeTestChanges(testInstance);
        }

        ToXContent.MapParams params = new ToXContent.MapParams(
            Collections.singletonMap(Metadata.CONTEXT_MODE_PARAM, randomFrom(CONTEXT_MODE_SNAPSHOT, CONTEXT_MODE_GATEWAY))
        );

        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference shuffled = toShuffledXContent(testInstance, xContentType, params, false);

        PersistentTasksCustomMetadata newInstance;
        try (XContentParser parser = createParser(xContentType.xContent(), shuffled)) {
            newInstance = doParseInstance(parser);
        }
        assertNotSame(newInstance, testInstance);

        assertEquals(testInstance.tasks().size(), newInstance.tasks().size());
        for (PersistentTask<?> testTask : testInstance.tasks()) {
            PersistentTask<TestParams> newTask = (PersistentTask<TestParams>) newInstance.getTask(testTask.getId());
            assertNotNull(newTask);

            // Things that should be serialized
            assertEquals(testTask.getTaskName(), newTask.getTaskName());
            assertEquals(testTask.getId(), newTask.getId());
            assertEquals(testTask.getState(), newTask.getState());
            assertEquals(testTask.getParams(), newTask.getParams());

            // Things that shouldn't be serialized
            assertEquals(0, newTask.getAllocationId());
            assertNull(newTask.getExecutorNode());
        }
    }

    public void testBuilder() {
        PersistentTasksCustomMetadata persistentTasks = null;
        String lastKnownTask = "";
        for (int i = 0; i < randomIntBetween(10, 100); i++) {
            final Builder builder;
            if (randomBoolean()) {
                builder = PersistentTasksCustomMetadata.builder();
            } else {
                builder = PersistentTasksCustomMetadata.builder(persistentTasks);
            }
            boolean changed = false;
            for (int j = 0; j < randomIntBetween(1, 10); j++) {
                switch (randomInt(3)) {
                    case 0:
                        lastKnownTask = addRandomTask(builder);
                        changed = true;
                        break;
                    case 1:
                        if (builder.hasTask(lastKnownTask)) {
                            changed = true;
                            builder.reassignTask(lastKnownTask, randomAssignment());
                        } else {
                            String fLastKnownTask = lastKnownTask;
                            expectThrows(ResourceNotFoundException.class, () -> builder.reassignTask(fLastKnownTask, randomAssignment()));
                        }
                        break;
                    case 2:
                        if (builder.hasTask(lastKnownTask)) {
                            changed = true;
                            builder.updateTaskState(lastKnownTask, randomBoolean() ? new State(randomAlphaOfLength(10)) : null);
                        } else {
                            String fLastKnownTask = lastKnownTask;
                            expectThrows(ResourceNotFoundException.class, () -> builder.updateTaskState(fLastKnownTask, null));
                        }
                        break;
                    case 3:
                        if (builder.hasTask(lastKnownTask)) {
                            changed = true;
                            builder.removeTask(lastKnownTask);
                        } else {
                            String fLastKnownTask = lastKnownTask;
                            expectThrows(ResourceNotFoundException.class, () -> builder.removeTask(fLastKnownTask));
                        }
                        break;
                }
            }
            assertEquals(changed, builder.isChanged());
            persistentTasks = builder.build();
        }
    }

    public void testMinVersionSerialization() throws IOException {
        PersistentTasksCustomMetadata.Builder tasks = PersistentTasksCustomMetadata.builder();

        Version minVersion = allReleasedVersions().stream().filter(Version::isRelease).findFirst().orElseThrow(NoSuchElementException::new);
        final Version streamVersion = randomVersionBetween(random(), minVersion, VersionUtils.getPreviousVersion(Version.CURRENT));
        tasks.addTask(
            "test_compatible_version",
            TestPersistentTasksExecutor.NAME,
            new TestParams(
                null,
                randomVersionBetween(random(), minVersion, streamVersion),
                randomBoolean() ? Optional.empty() : Optional.of("test")
            ),
            randomAssignment()
        );
        tasks.addTask(
            "test_incompatible_version",
            TestPersistentTasksExecutor.NAME,
            new TestParams(
                null,
                randomVersionBetween(random(), compatibleFutureVersion(streamVersion), Version.CURRENT),
                randomBoolean() ? Optional.empty() : Optional.of("test")
            ),
            randomAssignment()
        );
        final BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(streamVersion);
        Set<String> features = new HashSet<>();
        if (randomBoolean()) {
            features.add("test");
        }
        out.setFeatures(features);
        tasks.build().writeTo(out);

        final StreamInput input = out.bytes().streamInput();
        input.setVersion(streamVersion);
        PersistentTasksCustomMetadata read = new PersistentTasksCustomMetadata(
            new NamedWriteableAwareStreamInput(input, getNamedWriteableRegistry())
        );

        assertThat(read.taskMap().keySet(), equalTo(Collections.singleton("test_compatible_version")));
    }

    public void testDisassociateDeadNodes_givenNoPersistentTasks() {
        ClusterState originalState = ClusterState.builder(new ClusterName("persistent-tasks-tests")).build();
        ClusterState returnedState = PersistentTasksCustomMetadata.disassociateDeadNodes(originalState);
        assertThat(originalState, sameInstance(returnedState));
    }

    public void testDisassociateDeadNodes_givenAssignedPersistentTask() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT))
            .localNodeId("node1")
            .clusterManagerNodeId("node1")
            .build();

        String taskName = "test/task";
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder()
            .addTask(
                "task-id",
                taskName,
                emptyTaskParams(taskName),
                new PersistentTasksCustomMetadata.Assignment("node1", "test assignment")
            );

        ClusterState originalState = ClusterState.builder(new ClusterName("persistent-tasks-tests"))
            .nodes(nodes)
            .metadata(Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()))
            .build();
        ClusterState returnedState = PersistentTasksCustomMetadata.disassociateDeadNodes(originalState);
        assertThat(originalState, sameInstance(returnedState));

        PersistentTasksCustomMetadata originalTasks = PersistentTasksCustomMetadata.getPersistentTasksCustomMetadata(originalState);
        PersistentTasksCustomMetadata returnedTasks = PersistentTasksCustomMetadata.getPersistentTasksCustomMetadata(returnedState);
        assertEquals(originalTasks, returnedTasks);
    }

    public void testDisassociateDeadNodes() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT))
            .localNodeId("node1")
            .clusterManagerNodeId("node1")
            .build();

        String taskName = "test/task";
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder()
            .addTask(
                "assigned-task",
                taskName,
                emptyTaskParams(taskName),
                new PersistentTasksCustomMetadata.Assignment("node1", "test assignment")
            )
            .addTask(
                "task-on-deceased-node",
                taskName,
                emptyTaskParams(taskName),
                new PersistentTasksCustomMetadata.Assignment("left-the-cluster", "test assignment")
            );

        ClusterState originalState = ClusterState.builder(new ClusterName("persistent-tasks-tests"))
            .nodes(nodes)
            .metadata(Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()))
            .build();
        ClusterState returnedState = PersistentTasksCustomMetadata.disassociateDeadNodes(originalState);
        assertThat(originalState, not(sameInstance(returnedState)));

        PersistentTasksCustomMetadata originalTasks = PersistentTasksCustomMetadata.getPersistentTasksCustomMetadata(originalState);
        PersistentTasksCustomMetadata returnedTasks = PersistentTasksCustomMetadata.getPersistentTasksCustomMetadata(returnedState);
        assertNotEquals(originalTasks, returnedTasks);

        assertEquals(originalTasks.getTask("assigned-task"), returnedTasks.getTask("assigned-task"));
        assertNotEquals(originalTasks.getTask("task-on-deceased-node"), returnedTasks.getTask("task-on-deceased-node"));
        assertEquals(PersistentTasksCustomMetadata.LOST_NODE_ASSIGNMENT, returnedTasks.getTask("task-on-deceased-node").getAssignment());
    }

    public void testTaskCountsStayInStepWithTasks() {
        List<String> taskNames = Arrays.asList("task_a", "task_b", "task_c");
        List<String> nodes = Arrays.asList("node_1", "node_2", "node_3", "node_4");
        PersistentTasksCustomMetadata current = null;
        int taskSeq = 0;
        for (int round = 0; round < 50; round++) {
            if (current != null && randomBoolean()) {
                // the source has (or has not) computed its counts yet; the builder must be right either way
                current.getNumberOfTasksOnNode(randomFrom(nodes), randomFrom(taskNames));
            }
            Builder builder = randomBoolean() ? PersistentTasksCustomMetadata.builder(current) : PersistentTasksCustomMetadata.builder();
            PersistentTasksCustomMetadata previous = builder.build();
            Map<String, PersistentTask<?>> previousTasks = new HashMap<>(previous.taskMap());
            for (int op = 0; op < randomIntBetween(1, 30); op++) {
                List<String> ids = new ArrayList<>(builder.getCurrentTaskIds());
                Assignment assignment = randomBoolean() ? new Assignment(null, "unassigned") : new Assignment(randomFrom(nodes), "");
                int choice = ids.isEmpty() ? 0 : randomInt(3);
                switch (choice) {
                    case 0:
                        String name = randomFrom(taskNames);
                        builder.addTask("task_" + taskSeq++, name, emptyTaskParams(name), assignment);
                        break;
                    case 1:
                        builder.reassignTask(randomFrom(ids), assignment);
                        break;
                    case 2:
                        builder.updateTaskState(randomFrom(ids), null);
                        break;
                    default:
                        builder.removeTask(randomFrom(ids));
                }
                if (randomInt(5) == 0) {
                    assertCountsMatchTasks(builder.build(), taskNames, nodes);
                }
            }
            current = builder.build();
            assertCountsMatchTasks(current, taskNames, nodes);
            // instances handed out earlier are not affected by later builder mutations
            assertEquals(previousTasks, previous.taskMap());
            assertCountsMatchTasks(previous, taskNames, nodes);
        }
    }

    public void testTaskCountsDoNotAffectEqualsOrSerialization() throws IOException {
        Builder builder = PersistentTasksCustomMetadata.builder();
        for (int i = 0; i < 20; i++) {
            builder.addTask("task_" + i, TestPersistentTasksExecutor.NAME, new TestParams("p" + i), new Assignment("node_" + (i % 3), ""));
        }
        PersistentTasksCustomMetadata withCounts = builder.build();
        assertEquals(7, withCounts.getNumberOfTasksOnNode("node_0", TestPersistentTasksExecutor.NAME));
        PersistentTasksCustomMetadata withoutCounts = new PersistentTasksCustomMetadata(
            builder.getLastAllocationId(),
            new HashMap<>(withCounts.taskMap())
        );
        assertEquals(withCounts, withoutCounts);
        assertEquals(withCounts.hashCode(), withoutCounts.hashCode());

        BytesStreamOutput out = new BytesStreamOutput();
        withCounts.writeTo(out);
        BytesStreamOutput outWithout = new BytesStreamOutput();
        withoutCounts.writeTo(outWithout);
        assertEquals(out.bytes(), outWithout.bytes());
        StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), getNamedWriteableRegistry());
        PersistentTasksCustomMetadata read = new PersistentTasksCustomMetadata(in);
        assertEquals(withCounts, read);
        assertEquals(7, read.getNumberOfTasksOnNode("node_0", TestPersistentTasksExecutor.NAME));
        assertEquals(0, read.getNumberOfTasksOnNode("node_0", "other_task"));
        assertEquals(0, read.getNumberOfTasksOnNode("unknown_node", TestPersistentTasksExecutor.NAME));
    }

    private static void assertCountsMatchTasks(PersistentTasksCustomMetadata metadata, List<String> taskNames, List<String> nodes) {
        for (String taskName : taskNames) {
            for (String node : nodes) {
                long expected = metadata.tasks()
                    .stream()
                    .filter(t -> taskName.equals(t.getTaskName()) && node.equals(t.getExecutorNode()))
                    .count();
                assertEquals(taskName + " on " + node, expected, metadata.getNumberOfTasksOnNode(node, taskName));
            }
        }
        assertEquals(PersistentTasksCustomMetadata.countTasks(metadata.tasks()), metadata.taskCountsForTesting());
    }

    private PersistentTaskParams emptyTaskParams(String taskName) {
        return new PersistentTaskParams() {

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) {
                return builder;
            }

            @Override
            public void writeTo(StreamOutput out) {

            }

            @Override
            public String getWriteableName() {
                return taskName;
            }

            @Override
            public Version getMinimalSupportedVersion() {
                return Version.CURRENT;
            }
        };
    }

    private Assignment randomAssignment() {
        if (randomBoolean()) {
            if (randomBoolean()) {
                return NO_NODE_FOUND;
            } else {
                return new Assignment(null, randomAlphaOfLength(10));
            }
        }
        return new Assignment(randomAlphaOfLength(10), randomAlphaOfLength(10));
    }
}
