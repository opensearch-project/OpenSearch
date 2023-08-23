/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.opensearch.common.util.concurrent.ThreadContextStatePropagator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.tasks.TaskResourceTrackingService.TASK_ID;

/**
 * Propagates TASK_ID across thread contexts
 */
public class TaskThreadContextStatePropagator implements ThreadContextStatePropagator {
    @Override
    public Map<String, Object> transients(Map<String, Object> source) {
        final Map<String, Object> transients = new HashMap<>();

        if (source.containsKey(TASK_ID)) {
            transients.put(TASK_ID, source.get(TASK_ID));
        }

        return transients;
    }

    @Override
    public Map<String, String> headers(Map<String, Object> source) {
        return Collections.emptyMap();
    }
}
