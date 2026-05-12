/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DatafusionContextCancellationTests extends OpenSearchTestCase {

    private SearchShardTask createTask() {
        return new SearchShardTask(1L, "type", "action", "desc", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
    }

    private DatafusionContext createContext(Task task) {
        DatafusionReader reader = mock(DatafusionReader.class);
        when(reader.getReaderHandle()).thenReturn(null);
        return new DatafusionContext(task, reader, null);
    }

    public void testIsCancelledReturnsFalseWhenTaskNotCancelled() {
        SearchShardTask task = createTask();
        DatafusionContext ctx = createContext(task);
        assertFalse(ctx.isCancelled());
    }

    public void testIsCancelledReturnsTrueWhenTaskCancelled() {
        SearchShardTask task = createTask();
        DatafusionContext ctx = createContext(task);
        task.cancel("test");
        assertTrue(ctx.isCancelled());
    }

    public void testIsCancelledReturnsFalseWhenTaskIsNull() {
        DatafusionContext ctx = createContext(null);
        assertFalse(ctx.isCancelled());
    }

    public void testIsCancelledReturnsFalseWhenTaskIsNotSearchShardTask() {
        Task plainTask = new Task(1L, "type", "action", "desc", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
        DatafusionContext ctx = createContext(plainTask);
        assertFalse(ctx.isCancelled());
    }

    public void testGetContextIdReturnsZeroWhenNoQuery() {
        DatafusionContext ctx = createContext(null);
        assertEquals(0L, ctx.getContextId());
    }

    public void testGetContextIdReturnsQueryContextId() {
        DatafusionContext ctx = createContext(null);
        ctx.setDatafusionQuery(new DatafusionQuery("table", new byte[0], 42L));
        assertEquals(42L, ctx.getContextId());
    }

    public void testGetContextIdReturnsZeroForZeroContextQuery() {
        DatafusionContext ctx = createContext(null);
        ctx.setDatafusionQuery(new DatafusionQuery("table", new byte[0], 0L));
        assertEquals(0L, ctx.getContextId());
    }
}
