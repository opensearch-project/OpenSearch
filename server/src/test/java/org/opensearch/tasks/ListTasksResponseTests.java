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

package org.opensearch.tasks;

import org.opensearch.OpenSearchException;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.TaskOperationFailure;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.common.Strings;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class ListTasksResponseTests extends AbstractXContentTestCase<ListTasksResponse> {

    public void testEmptyToString() {
        assertEquals("{\n" + "  \"tasks\" : [ ]\n" + "}", new ListTasksResponse(null, null, null).toString());
    }

    public void testNonEmptyToString() {
        TaskInfo info = new TaskInfo(
            new TaskId("node1", 1),
            "dummy-type",
            "dummy-action",
            "dummy-description",
            null,
            0,
            1,
            true,
            false,
            new TaskId("node1", 0),
            Collections.singletonMap("foo", "bar"),
            new TaskResourceStats(new HashMap<String, TaskResourceUsage>() {
                {
                    put("dummy-type1", new TaskResourceUsage(100, 100));
                }
            })
        );
        ListTasksResponse tasksResponse = new ListTasksResponse(singletonList(info), emptyList(), emptyList());
        assertEquals(
            "{\n"
                + "  \"tasks\" : [\n"
                + "    {\n"
                + "      \"node\" : \"node1\",\n"
                + "      \"id\" : 1,\n"
                + "      \"type\" : \"dummy-type\",\n"
                + "      \"action\" : \"dummy-action\",\n"
                + "      \"description\" : \"dummy-description\",\n"
                + "      \"start_time\" : \"1970-01-01T00:00:00.000Z\",\n"
                + "      \"start_time_in_millis\" : 0,\n"
                + "      \"running_time\" : \"1nanos\",\n"
                + "      \"running_time_in_nanos\" : 1,\n"
                + "      \"cancellable\" : true,\n"
                + "      \"cancelled\" : false,\n"
                + "      \"parent_task_id\" : \"node1:0\",\n"
                + "      \"headers\" : {\n"
                + "        \"foo\" : \"bar\"\n"
                + "      },\n"
                + "      \"resource_stats\" : {\n"
                + "        \"dummy-type1\" : {\n"
                + "          \"cpu_time_in_nanos\" : 100,\n"
                + "          \"memory_in_bytes\" : 100\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}",
            tasksResponse.toString()
        );
    }

    @Override
    protected ListTasksResponse createTestInstance() {
        // failures are tested separately, so we can test xcontent equivalence at least when we have no failures
        return new ListTasksResponse(randomTasks(), Collections.emptyList(), Collections.emptyList());
    }

    private static List<TaskInfo> randomTasks() {
        List<TaskInfo> tasks = new ArrayList<>();
        for (int i = 0; i < randomInt(10); i++) {
            tasks.add(TaskInfoTests.randomTaskInfo());
        }
        return tasks;
    }

    @Override
    protected ListTasksResponse doParseInstance(XContentParser parser) {
        return ListTasksResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // status, headers and resource_stats hold arbitrary content, we can't inject random fields in them
        return field -> field.endsWith("status") || field.endsWith("headers") || field.contains("resource_stats");
    }

    @Override
    protected void assertEqualInstances(ListTasksResponse expectedInstance, ListTasksResponse newInstance) {
        assertNotSame(expectedInstance, newInstance);
        assertThat(newInstance.getTasks(), equalTo(expectedInstance.getTasks()));
        assertOnNodeFailures(newInstance.getNodeFailures(), expectedInstance.getNodeFailures());
        assertOnTaskFailures(newInstance.getTaskFailures(), expectedInstance.getTaskFailures());
    }

    protected static void assertOnNodeFailures(List<OpenSearchException> nodeFailures, List<OpenSearchException> expectedFailures) {
        assertThat(nodeFailures.size(), equalTo(expectedFailures.size()));
        for (int i = 0; i < nodeFailures.size(); i++) {
            OpenSearchException newException = nodeFailures.get(i);
            OpenSearchException expectedException = expectedFailures.get(i);
            assertThat(newException.getMetadata("opensearch.node_id").get(0), equalTo(((FailedNodeException) expectedException).nodeId()));
            assertThat(newException.getMessage(), equalTo("OpenSearch exception [type=failed_node_exception, reason=error message]"));
            assertThat(newException.getCause(), instanceOf(OpenSearchException.class));
            OpenSearchException cause = (OpenSearchException) newException.getCause();
            assertThat(cause.getMessage(), equalTo("OpenSearch exception [type=connect_exception, reason=null]"));
        }
    }

    protected static void assertOnTaskFailures(List<TaskOperationFailure> taskFailures, List<TaskOperationFailure> expectedFailures) {
        assertThat(taskFailures.size(), equalTo(expectedFailures.size()));
        for (int i = 0; i < taskFailures.size(); i++) {
            TaskOperationFailure newFailure = taskFailures.get(i);
            TaskOperationFailure expectedFailure = expectedFailures.get(i);
            assertThat(newFailure.getNodeId(), equalTo(expectedFailure.getNodeId()));
            assertThat(newFailure.getTaskId(), equalTo(expectedFailure.getTaskId()));
            assertThat(newFailure.getStatus(), equalTo(expectedFailure.getStatus()));
            assertThat(newFailure.getCause(), instanceOf(OpenSearchException.class));
            OpenSearchException cause = (OpenSearchException) newFailure.getCause();
            assertThat(cause.getMessage(), equalTo("OpenSearch exception [type=illegal_state_exception, reason=null]"));
        }
    }

    /**
     * Test parsing {@link ListTasksResponse} with inner failures as they don't support asserting on xcontent equivalence, given that
     * exceptions are not parsed back as the same original class. We run the usual {@link AbstractXContentTestCase#testFromXContent()}
     * without failures, and this other test with failures where we disable asserting on xcontent equivalence at the end.
     */
    public void testFromXContentWithFailures() throws IOException {
        Supplier<ListTasksResponse> instanceSupplier = ListTasksResponseTests::createTestInstanceWithFailures;
        // with random fields insertion in the inner exceptions, some random stuff may be parsed back as metadata,
        // but that does not bother our assertions, as we only want to test that we don't break.
        boolean supportsUnknownFields = true;
        // exceptions are not of the same type whenever parsed back
        boolean assertToXContentEquivalence = false;
        AbstractXContentTestCase.testFromXContent(
            NUMBER_OF_TEST_RUNS,
            instanceSupplier,
            supportsUnknownFields,
            Strings.EMPTY_ARRAY,
            getRandomFieldsExcludeFilter(),
            this::createParser,
            this::doParseInstance,
            this::assertEqualInstances,
            assertToXContentEquivalence,
            ToXContent.EMPTY_PARAMS
        );
    }

    private static ListTasksResponse createTestInstanceWithFailures() {
        int numNodeFailures = randomIntBetween(0, 3);
        List<FailedNodeException> nodeFailures = new ArrayList<>(numNodeFailures);
        for (int i = 0; i < numNodeFailures; i++) {
            nodeFailures.add(new FailedNodeException(randomAlphaOfLength(5), "error message", new ConnectException()));
        }
        int numTaskFailures = randomIntBetween(0, 3);
        List<TaskOperationFailure> taskFailures = new ArrayList<>(numTaskFailures);
        for (int i = 0; i < numTaskFailures; i++) {
            taskFailures.add(new TaskOperationFailure(randomAlphaOfLength(5), randomLong(), new IllegalStateException()));
        }
        return new ListTasksResponse(randomTasks(), taskFailures, nodeFailures);
    }
}
