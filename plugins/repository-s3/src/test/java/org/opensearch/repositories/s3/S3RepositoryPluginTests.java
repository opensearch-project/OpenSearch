/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.SizeUnit;
import org.opensearch.common.unit.SizeValue;
import org.opensearch.common.util.concurrent.OpenSearchThreadPoolExecutor;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPool.ThreadPoolType;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executor;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class S3RepositoryPluginTests extends OpenSearchTestCase {

    private static final String URGENT_FUTURE_COMPLETION = "urgent_future_completion";

    public void testGetExecutorBuilders() throws IOException {
        final int processors = randomIntBetween(1, 64);
        Settings settings = Settings.builder().put("node.name", "test").put("node.processors", processors).build();
        Path configPath = createTempDir();
        ThreadPool threadPool = null;
        try (S3RepositoryPlugin plugin = new S3RepositoryPlugin(settings, configPath)) {
            List<ExecutorBuilder<?>> executorBuilders = plugin.getExecutorBuilders(settings);
            assertNotNull(executorBuilders);
            assertFalse(executorBuilders.isEmpty());
            threadPool = new ThreadPool(settings, executorBuilders.toArray(new ExecutorBuilder<?>[0]));
            final Executor executor = threadPool.executor(URGENT_FUTURE_COMPLETION);
            assertNotNull(executor);
            assertThat(executor, instanceOf(OpenSearchThreadPoolExecutor.class));
            final OpenSearchThreadPoolExecutor openSearchThreadPoolExecutor = (OpenSearchThreadPoolExecutor) executor;
            final ThreadPool.Info info = threadPool.info(URGENT_FUTURE_COMPLETION);
            int size = boundedBy((processors + 1) / 2, 1, 2);
            assertThat(info.getName(), equalTo(URGENT_FUTURE_COMPLETION));
            assertThat(info.getThreadPoolType(), equalTo(ThreadPoolType.FIXED));
            assertThat(info.getQueueSize(), notNullValue());
            assertThat(info.getQueueSize(), equalTo(new SizeValue(10, SizeUnit.KILO)));
            assertThat(openSearchThreadPoolExecutor.getQueue().remainingCapacity(), equalTo(10_000));

            assertThat(info.getMin(), equalTo(size));
            assertThat(openSearchThreadPoolExecutor.getCorePoolSize(), equalTo(size));
            assertThat(info.getMax(), equalTo(size));
            assertThat(openSearchThreadPoolExecutor.getMaximumPoolSize(), equalTo(size));

            final int availableProcessors = Runtime.getRuntime().availableProcessors();
            if (processors > availableProcessors) {
                assertWarnings(
                    "setting [node.processors] to value ["
                        + processors
                        + "] which is more than available processors ["
                        + availableProcessors
                        + "] is deprecated"
                );
            }
        } finally {
            if (threadPool != null) {
                terminate(threadPool);
            }
        }
    }

    private static int boundedBy(int value, int min, int max) {
        return Math.min(max, Math.max(min, value));
    }

}
