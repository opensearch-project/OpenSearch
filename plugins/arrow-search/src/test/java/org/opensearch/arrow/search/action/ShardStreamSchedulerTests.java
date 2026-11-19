/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.action;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

public class ShardStreamSchedulerTests extends OpenSearchTestCase {

    private final List<Runnable> executed = new ArrayList<>();

    private final class Launch implements ShardStreamScheduler.Launch {
        final String node;
        final List<String> log;

        Launch(String node, List<String> log) {
            this.node = node;
            this.log = log;
        }

        @Override
        public void run() {
            log.add("run " + node);
        }

        @Override
        public void abandon() {
            log.add("abandon " + node);
        }
    }

    private void runExecuted() {
        List<Runnable> tasks = new ArrayList<>(executed);
        executed.clear();
        tasks.forEach(Runnable::run);
    }

    public void testOpensAtMostTheLimitAndNeverRunsInline() {
        ShardStreamScheduler scheduler = new ShardStreamScheduler(2, executed::add);
        List<String> log = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            scheduler.submit("n" + i, new Launch("n" + i, log));
        }
        assertEquals("launches run on the executor", List.of(), log);
        runExecuted();
        assertEquals(List.of("run n0", "run n1"), log);
        assertEquals(2, scheduler.openStreams());

        scheduler.release("n0");
        runExecuted();
        assertEquals(List.of("run n0", "run n1", "run n2"), log);
        assertEquals(2, scheduler.openStreams());
    }

    public void testPrefersTheNodeWithFewestOpenStreams() {
        ShardStreamScheduler scheduler = new ShardStreamScheduler(3, executed::add);
        List<String> log = new ArrayList<>();
        scheduler.submit("a", new Launch("a", log));
        scheduler.submit("a", new Launch("a", log));
        scheduler.submit("a", new Launch("a", log));
        scheduler.submit("b", new Launch("b", log));
        scheduler.submit("b", new Launch("b", log));
        runExecuted();
        // The three launches on a fill the slots in submission order. When one ends, b runs next, since a still has two open.
        assertEquals(List.of("run a", "run a", "run a"), log);
        scheduler.release("a");
        runExecuted();
        assertEquals("run b", log.get(3));
        scheduler.release("a");
        runExecuted();
        assertEquals("a and b both have one open stream, so the queued b runs", "run b", log.get(4));
    }

    public void testCloseAbandonsQueuedAndLaterLaunches() {
        ShardStreamScheduler scheduler = new ShardStreamScheduler(1, executed::add);
        List<String> log = new ArrayList<>();
        scheduler.submit("a", new Launch("a", log));
        scheduler.submit("b", new Launch("b", log));
        runExecuted();
        scheduler.close();
        runExecuted();
        assertEquals(List.of("run a", "abandon b"), log);
        scheduler.submit("c", new Launch("c", log));
        scheduler.release("a");
        runExecuted();
        assertEquals(List.of("run a", "abandon b", "abandon c"), log);
        assertEquals(0, scheduler.openStreams());
    }
}
