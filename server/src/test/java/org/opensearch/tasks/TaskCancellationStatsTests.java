/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;

public class TaskCancellationStatsTests extends AbstractWireSerializingTestCase<TaskCancellationStats> {
    @Override
    protected Writeable.Reader<TaskCancellationStats> instanceReader() {
        return TaskCancellationStats::new;
    }

    @Override
    protected TaskCancellationStats createTestInstance() {
        return randomInstance();
    }

    public static TaskCancellationStats randomInstance() {
        return new TaskCancellationStats(SearchShardTaskCancellationStatsTests.randomInstance());
    }
}
