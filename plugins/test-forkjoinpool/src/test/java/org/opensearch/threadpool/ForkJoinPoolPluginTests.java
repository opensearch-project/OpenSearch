package org.opensearch.threadpool;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugin.threadpool.ForkJoinPoolPlugin;

import java.util.List;

/**
 * Unit tests for the {@link ForkJoinPoolPlugin} class.
 * <p>
 * This test verifies that the ForkJoinPoolPlugin registers the expected ForkJoinPoolExecutorBuilder
 * with the correct name and type.
 */
public class ForkJoinPoolPluginTests extends LuceneTestCase {

    /**
     * Tests that the ForkJoinPoolPlugin registers exactly one ForkJoinPoolExecutorBuilder
     * with the expected pool name "jvector".
     */
    public void testForkJoinPoolPluginRegistersBuilder() {
        ForkJoinPoolPlugin plugin = new ForkJoinPoolPlugin();
        List<ExecutorBuilder<?>> builders = plugin.getExecutorBuilders(Settings.EMPTY);

        // Should register exactly one builder (jvector)
        assertEquals("Plugin should register exactly one builder", 1, builders.size());
        ExecutorBuilder<?> builder = builders.get(0);

        // The builder should be of the correct type and name
        assertTrue("Builder should be of type ForkJoinPoolExecutorBuilder", builder instanceof ForkJoinPoolExecutorBuilder);
        assertEquals("Builder name should be 'jvector'", "jvector", builder.name());
    }
}
