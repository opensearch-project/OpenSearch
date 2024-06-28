/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.query;

import org.opensearch.search.profile.Timer;
import org.opensearch.test.OpenSearchTestCase;

import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ConcurrentQueryProfilerTests extends OpenSearchTestCase {

    public void testMergeRewriteTimeIntervals() {
        ConcurrentQueryProfiler profiler = new ConcurrentQueryProfiler(new ConcurrentQueryProfileTree());
        List<Timer> timers = new LinkedList<>();
        timers.add(new Timer(217134L, 1L, 1L, 0L, 553074511206907L));
        timers.add(new Timer(228954L, 1L, 1L, 0L, 553074509287335L));
        timers.add(new Timer(228954L, 1L, 1L, 0L, 553074509287336L));
        LinkedList<long[]> mergedIntervals = profiler.mergeRewriteTimeIntervals(timers);
        assertThat(mergedIntervals.size(), equalTo(2));
        long[] interval = mergedIntervals.get(0);
        assertThat(interval[0], equalTo(553074509287335L));
        assertThat(interval[1], equalTo(553074509516290L));
        interval = mergedIntervals.get(1);
        assertThat(interval[0], equalTo(553074511206907L));
        assertThat(interval[1], equalTo(553074511424041L));
    }
}
