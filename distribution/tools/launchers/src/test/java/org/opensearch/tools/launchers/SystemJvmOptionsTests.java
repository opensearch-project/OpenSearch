/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.launchers;

import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

public class SystemJvmOptionsTests extends LaunchersTestCase {

    private static final String PARALLELISM = "-Djdk.virtualThreadScheduler.parallelism=";

    /**
     * The virtual-thread scheduler must be given more carriers than the JDK default of one per CPU,
     * because a virtual thread that blocks with a native frame on its stack keeps its carrier and
     * enough of those starve the scheduler outright.
     *
     * <p>Asserted as the two properties the value is chosen for rather than as the expression itself:
     * a floor that holds on a small host, and a ratio that scales with a large one. The floor is the
     * part that was measured — 64 is what unwedged an 8-vCPU coordinator, where the ratio alone would
     * have given 32.
     */
    public void testVirtualThreadSchedulerHasHeadroomOverCpuCount() {
        List<String> options = SystemJvmOptions.systemJvmOptions()
            .stream()
            .filter(option -> option.startsWith(PARALLELISM))
            .collect(Collectors.toList());

        assertThat("exactly one parallelism option, else the last one silently wins", options, hasSize(1));

        int parallelism = Integer.parseInt(options.get(0).substring(PARALLELISM.length()));
        assertThat("floor: a small host still needs enough carriers to absorb pinned threads", parallelism, greaterThanOrEqualTo(64));
        assertThat(
            "ratio: the same headroom must survive on a large host",
            parallelism,
            greaterThanOrEqualTo(Runtime.getRuntime().availableProcessors() * 4)
        );
    }

    /**
     * Options are assembled from a fixed list in which some entries are conditional and return "".
     * Those placeholders must be filtered out, since an empty argument on the command line is not
     * inert — it makes the JVM fail to start.
     */
    public void testNoEmptyOptions() {
        assertThat(SystemJvmOptions.systemJvmOptions(), everyItem(not(equalTo(""))));
    }
}
