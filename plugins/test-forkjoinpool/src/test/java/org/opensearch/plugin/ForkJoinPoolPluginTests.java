/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.threadpool;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ForkJoinPoolExecutorBuilder;

import java.lang.reflect.Field;
import java.util.List;

public class ForkJoinPoolPluginTests extends OpenSearchTestCase {

    public void testRegistersForkJoinPool() throws Exception {
        ForkJoinPoolPlugin plugin = new ForkJoinPoolPlugin();
        List<ExecutorBuilder<?>> builders = plugin.getExecutorBuilders(Settings.EMPTY);

        assertEquals(1, builders.size());
        ExecutorBuilder<?> builder = builders.get(0);

        assertTrue(builder instanceof ForkJoinPoolExecutorBuilder);

        Field nameField = ExecutorBuilder.class.getDeclaredField("name");
        nameField.setAccessible(true);
        assertEquals("jvector", nameField.get(builder));
    }
}
