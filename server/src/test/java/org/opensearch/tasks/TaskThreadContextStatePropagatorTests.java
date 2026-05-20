/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.tasks.TaskResourceTrackingService.TASK_ID;

public class TaskThreadContextStatePropagatorTests extends OpenSearchTestCase {
    private final TaskThreadContextStatePropagator taskThreadContextStatePropagator = new TaskThreadContextStatePropagator();

    public void testTransient() {
        Map<String, Object> transientHeader = new HashMap<>();
        transientHeader.put(TASK_ID, "t_1");
        Map<String, Object> transientPropagatedHeader = taskThreadContextStatePropagator.transients(transientHeader, false);
        assertEquals("t_1", transientPropagatedHeader.get(TASK_ID));
    }

    public void testTransientForSystemContext() {
        Map<String, Object> transientHeader = new HashMap<>();
        transientHeader.put(TASK_ID, "t_1");
        Map<String, Object> transientPropagatedHeader = taskThreadContextStatePropagator.transients(transientHeader, true);
        assertEquals("t_1", transientPropagatedHeader.get(TASK_ID));
    }
}
