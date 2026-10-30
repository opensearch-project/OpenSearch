/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.search.SearchService;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.node.NodeClient;

import java.util.Collections;

import static org.mockito.Mockito.mock;

public class TimeoutTaskCancellationUtilityTests extends OpenSearchTestCase {

    private CancellableTask createCancellableTask(TimeValue cancelAfterTimeInterval) {
        return new CancellableTask(
            1L,
            "test_type",
            "test_action",
            "test_description",
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap(),
            cancelAfterTimeInterval
        ) {
            @Override
            public boolean shouldCancelChildren() {
                return false;
            }
        };
    }

    public void testNegativeIntervalReturnsOriginalListener() {
        NodeClient client = mock(NodeClient.class);
        ActionListener<Object> originalListener = ActionListener.wrap(r -> {}, e -> {});

        TimeValue[] negativeIntervals = new TimeValue[] {
            SearchService.NO_TIMEOUT,
            TimeValue.timeValueMillis(-1),
            TimeValue.timeValueSeconds(-1),
            TimeValue.timeValueMinutes(-1),
            TimeValue.timeValueHours(-1)
        };

        for (TimeValue interval : negativeIntervals) {
            CancellableTask task = createCancellableTask(interval);
            ActionListener<Object> wrapped = TimeoutTaskCancellationUtility.wrapWithCancellationListener(
                client,
                task,
                TimeValue.timeValueSeconds(10),
                originalListener,
                e -> {}
            );
            assertSame("Expected original listener for interval: " + interval, originalListener, wrapped);
        }
    }

    public void testZeroIntervalReturnsOriginalListener() {
        NodeClient client = mock(NodeClient.class);
        ActionListener<Object> originalListener = ActionListener.wrap(r -> {}, e -> {});

        TimeValue[] zeroIntervals = new TimeValue[] {
            TimeValue.ZERO,
            TimeValue.timeValueMillis(0),
            TimeValue.timeValueSeconds(0),
            TimeValue.timeValueMinutes(0)
        };

        for (TimeValue interval : zeroIntervals) {
            CancellableTask task = createCancellableTask(interval);
            ActionListener<Object> wrapped = TimeoutTaskCancellationUtility.wrapWithCancellationListener(
                client,
                task,
                TimeValue.timeValueSeconds(10),
                originalListener,
                e -> {}
            );
            assertSame("Expected original listener for zero interval: " + interval, originalListener, wrapped);
        }
    }

    public void testNullIntervalWithNegativeOrZeroDefaultReturnsOriginalListener() {
        NodeClient client = mock(NodeClient.class);
        ActionListener<Object> originalListener = ActionListener.wrap(r -> {}, e -> {});
        CancellableTask task = createCancellableTask(null);

        TimeValue[] nonPositiveDefaults = new TimeValue[] {
            null,
            SearchService.NO_TIMEOUT,
            TimeValue.timeValueSeconds(-1),
            TimeValue.timeValueMinutes(-1),
            TimeValue.ZERO,
            TimeValue.timeValueSeconds(0)
        };

        for (TimeValue defaultTimeout : nonPositiveDefaults) {
            ActionListener<Object> wrapped = TimeoutTaskCancellationUtility.wrapWithCancellationListener(
                client,
                task,
                defaultTimeout,
                originalListener,
                e -> {}
            );
            assertSame("Expected original listener for default timeout: " + defaultTimeout, originalListener, wrapped);
        }
    }
}
