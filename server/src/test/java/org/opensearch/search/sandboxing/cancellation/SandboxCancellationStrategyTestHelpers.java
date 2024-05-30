/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandboxing.cancellation;

import org.opensearch.action.search.SearchAction;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.tasks.resourcetracker.ResourceStats;
import org.opensearch.core.tasks.resourcetracker.ResourceStatsType;
import org.opensearch.core.tasks.resourcetracker.ResourceUsageMetric;
import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.test.OpenSearchTestCase.randomLong;
import static org.opensearch.test.OpenSearchTestCase.randomLongBetween;

public class SandboxCancellationStrategyTestHelpers {

  public static List<Task> getListOfTasks(long totalMemory) {
    List<Task> tasks = new ArrayList<>();

    while (totalMemory > 0) {
      long id = randomLong();
      final Task task = getRandomTask(id);
      long initial_memory = randomLongBetween(1, 100);

      ResourceUsageMetric[] initialTaskResourceMetrics = new ResourceUsageMetric[]{
          new ResourceUsageMetric(ResourceStats.MEMORY, initial_memory)
      };
      task.startThreadResourceTracking(id, ResourceStatsType.WORKER_STATS, initialTaskResourceMetrics);

      long memory = initial_memory + randomLongBetween(1, 10000);

      totalMemory -= memory - initial_memory;

      ResourceUsageMetric[] taskResourceMetrics = new ResourceUsageMetric[]{
          new ResourceUsageMetric(ResourceStats.MEMORY, memory),
      };
      task.updateThreadResourceStats(id, ResourceStatsType.WORKER_STATS, taskResourceMetrics);
      task.stopThreadResourceTracking(id, ResourceStatsType.WORKER_STATS);
      tasks.add(task);
    }

    return tasks;
  }

  public static Task getRandomTask(long id) {
    return new Task(
        id,
        "transport",
        SearchAction.NAME,
        "test description",
        new TaskId(randomLong() + ":" + randomLong()),
        Collections.emptyMap()
    );
  }
}
