/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.plugin.threadpool;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ForkJoinPoolExecutorBuilder;

import java.util.List;

public class ForkJoinPoolPluginTests extends OpenSearchTestCase {

    public void testRegistersForkJoinPool() {
        ForkJoinPoolPlugin plugin = new ForkJoinPoolPlugin();
        List<ExecutorBuilder<?>> builders = plugin.getExecutorBuilders(Settings.EMPTY);

        assertEquals(1, builders.size());
        ExecutorBuilder<?> builder = builders.get(0);

        assertTrue(builder instanceof ForkJoinPoolExecutorBuilder);
    }
}
