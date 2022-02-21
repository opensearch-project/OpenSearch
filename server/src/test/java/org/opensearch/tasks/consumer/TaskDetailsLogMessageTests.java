/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks.consumer;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class TaskDetailsLogMessageTests extends OpenSearchSingleNodeTestCase {
    public void testTaskDetailsLogHasJsonFields() {
        Task task = new SearchShardTask(
            0,
            "n/a",
            "n/a",
            "test",
            null,
            Collections.singletonMap(Task.X_OPAQUE_ID, "my_id"),
            () -> "test_metadata"
        );
        TaskDetailsLogMessage p = new TaskDetailsLogMessage(task);

        assertThat(p.getValueFor("taskId"), equalTo("0"));
        assertThat(p.getValueFor("type"), equalTo("n/a"));
        assertThat(p.getValueFor("action"), equalTo("n/a"));
        assertThat(p.getValueFor("description"), equalTo("test"));
        assertThat(p.getValueFor("parentTaskId"), equalTo(null));
        assertThat(p.getValueFor("resource_stats"), equalTo("{memory=100, cpu=100}"));
        assertThat(p.getValueFor("metadata"), equalTo("test_metadata"));
    }
}
