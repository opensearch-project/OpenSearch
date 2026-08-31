/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit coverage for {@link SnapshotsService#withRepositoryIoTimeout(ActionListener, String)} — the wrapper that turns a
 * cluster-manager-side repository call that never returns into a failure.
 */
public class SnapshotRepositoryIoTimeoutTests extends OpenSearchTestCase {

    private TestThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    private SnapshotsService newSnapshotsService(Settings extraSettings) {
        final ClusterService clusterService = mock(ClusterService.class);
        final Settings settings = Settings.builder()
            .put("node.name", "test")
            .putList("node.roles", "cluster_manager", "data")
            .put(extraSettings)
            .build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        final TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);

        return new SnapshotsService(
            settings,
            clusterService,
            mock(org.opensearch.cluster.metadata.IndexNameExpressionResolver.class),
            mock(org.opensearch.repositories.RepositoriesService.class),
            transportService,
            mock(org.opensearch.action.support.ActionFilters.class),
            null,
            new org.opensearch.indices.RemoteStoreSettings(Settings.EMPTY, clusterSettings),
            null
        );
    }

    public void testWrapIsIdentityWhenFeatureFlagDisabled() {
        final SnapshotsService snapshotsService = newSnapshotsService(Settings.EMPTY);
        final ActionListener<String> listener = ActionListener.wrap(r -> {}, e -> {});

        assertSame(
            "with the feature flag off the listener must be returned unwrapped, preserving the pre-existing code path",
            listener,
            snapshotsService.withRepositoryIoTimeout(listener, "test")
        );
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testWrapFailsListenerAfterTimeout() throws Exception {
        final SnapshotsService snapshotsService = newSnapshotsService(
            Settings.builder().put(SnapshotsService.SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(1)).build()
        );
        assertEquals(TimeValue.timeValueSeconds(1), snapshotsService.getIoTimeout());

        final CountDownLatch failed = new CountDownLatch(1);
        final AtomicReference<Exception> failure = new AtomicReference<>();
        final ActionListener<String> inner = ActionListener.wrap(r -> { throw new AssertionError("unexpected response"); }, e -> {
            failure.set(e);
            failed.countDown();
        });
        final ActionListener<String> wrapped = snapshotsService.withRepositoryIoTimeout(inner, "hung repository call");

        assertNotSame("with the flag on the listener must be wrapped", inner, wrapped);
        assertTrue("listener should have been failed by the timeout", failed.await(30, TimeUnit.SECONDS));
        assertThat(failure.get(), instanceOf(OpenSearchTimeoutException.class));
        assertThat(failure.get().getMessage(), containsString("hung repository call"));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testLateCompletionAfterTimeoutIsDropped() throws Exception {
        final SnapshotsService snapshotsService = newSnapshotsService(
            Settings.builder().put(SnapshotsService.SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(1)).build()
        );

        final AtomicInteger invocations = new AtomicInteger();
        final CountDownLatch failed = new CountDownLatch(1);
        final ActionListener<String> wrapped = snapshotsService.withRepositoryIoTimeout(
            ActionListener.wrap(r -> { invocations.incrementAndGet(); }, e -> {
                invocations.incrementAndGet();
                failed.countDown();
            }),
            "hung repository call"
        );

        assertTrue(failed.await(30, TimeUnit.SECONDS));

        // The orphaned repository worker finishes late. It must not reach the business listener a second time.
        wrapped.onResponse("late");
        wrapped.onFailure(new IllegalStateException("late failure"));

        assertEquals("business listener must be invoked exactly once", 1, invocations.get());
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testResponseBeforeTimeoutIsDeliveredAndTimeoutCancelled() throws Exception {
        final SnapshotsService snapshotsService = newSnapshotsService(
            Settings.builder().put(SnapshotsService.SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING.getKey(), TimeValue.timeValueMinutes(30)).build()
        );

        final AtomicInteger responses = new AtomicInteger();
        final AtomicInteger failures = new AtomicInteger();
        final ActionListener<String> wrapped = snapshotsService.withRepositoryIoTimeout(
            ActionListener.wrap(r -> responses.incrementAndGet(), e -> failures.incrementAndGet()),
            "healthy repository call"
        );

        wrapped.onResponse("ok");

        assertEquals(1, responses.get());
        assertEquals(0, failures.get());
    }
}
