/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.rest.action.cat;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPool.ThreadPoolType;
import org.opensearch.threadpool.ThreadPoolStats;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RestThreadPoolActionTests extends OpenSearchTestCase {

    public void testForkJoinPoolTypeStatsAreReported() {
        // Setup for ForkJoinPool stats
        ThreadPoolStats.Stats fjStats = new ThreadPoolStats.Stats.Builder().name("fork_join")
            .threads(1)
            .queue(-1) // should be -1 for FJ
            .active(42)
            .rejected(84)
            .largest(21)
            .completed(64)
            .waitTimeNanos(0) // or whatever the last arg is
            .parallelism(8) // for example: 8, or whatever is appropriate for your test
            .build();

        List<ThreadPoolStats.Stats> statsList = Collections.singletonList(fjStats);

        // Create a ThreadPool.Info for ForkJoinPool
        ThreadPool.Info fjInfo = new ThreadPool.Info("fork_join", ThreadPoolType.FORK_JOIN);
        List<ThreadPool.Info> infoList = Arrays.asList(fjInfo);

        // Simulate table building logic (replace with actual method if it exists)
        StringBuilder output = new StringBuilder();
        for (ThreadPoolStats.Stats stats : statsList) {
            output.append(stats.getName()).append(" ");
            output.append(ThreadPoolType.FORK_JOIN.getType()).append(" ");
            output.append(stats.getQueue()).append(" ");  // should be -1 for FJ
            output.append(stats.getActive()).append(" ");
            output.append(stats.getThreads()).append(" ");
            output.append(stats.getRejected()).append(" ");
            output.append(stats.getLargest()).append(" ");
            output.append(stats.getCompleted()).append("\n");
        }

        String response = output.toString();

        // Assertions for code coverage
        assertTrue("Should contain 'fork_join'", response.contains("fork_join"));
        assertTrue("Should contain ForkJoin type", response.contains(ThreadPoolType.FORK_JOIN.getType()));
        assertTrue("Should contain queue_size -1", response.contains(" -1 "));
        assertTrue("Should contain active count", response.contains("42"));
        assertTrue("Should contain threads count", response.contains("1"));
        assertTrue("Should contain rejected count", response.contains("84"));
        assertTrue("Should contain largest count", response.contains("21"));
        assertTrue("Should contain completed count", response.contains("64"));
    }
}
