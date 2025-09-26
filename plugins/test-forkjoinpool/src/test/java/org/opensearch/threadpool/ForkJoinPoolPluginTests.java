/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.threadpool;

import org.opensearch.common.settings.Settings;
import org.opensearch.plugin.threadpool.ForkJoinPoolPlugin;
import org.junit.Assert;

import java.util.List;

/**
 * Unit tests for the {@link ForkJoinPoolPlugin} class.
 * <p>
 * This test verifies that the ForkJoinPoolPlugin registers the expected ForkJoinPoolExecutorBuilder
 * with the correct name and type.
 */
public class ForkJoinPoolPluginTests {

    /**
     * Tests that the ForkJoinPoolPlugin registers exactly one ForkJoinPoolExecutorBuilder
     * with the expected pool name "jvector".
     */
    public void testForkJoinPoolPluginRegistersBuilder() {
        ForkJoinPoolPlugin plugin = new ForkJoinPoolPlugin();
        List<ExecutorBuilder<?>> builders = plugin.getExecutorBuilders(Settings.EMPTY);

        // Should register exactly one builder (jvector)
        Assert.assertEquals("Plugin should register exactly one builder", 1, builders.size());
        ExecutorBuilder<?> builder = builders.get(0);

        // The builder should be of the correct type and name
        Assert.assertTrue("Builder should be of type ForkJoinPoolExecutorBuilder", builder instanceof ForkJoinPoolExecutorBuilder);
        Assert.assertEquals("Builder name should be 'jvector'", "jvector", builder.name());
    }
}
