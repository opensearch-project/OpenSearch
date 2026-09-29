/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.coordination.DeterministicTaskQueue;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SnapshotsService#withRepositoryIoTimeout}, the instance seam that every repository I/O call
 * site goes through to put a time budget on the listener it hands the repository.
 * <p>
 * No test here reaches a call site: {@code SnapshotResiliencyTests} and {@code PromotedSnapshotDeleteTests} cover the delete
 * site's expiry, and {@code SnapshotRepositoryIoTimeoutTests} drives the static {@code withIoTimeout}; this file drives only
 * what the instance seam adds.
 * <p>
 * Every timer runs on a {@link DeterministicTaskQueue}, so expiry is ordered against the rest of each test rather than
 * raced against it, and every callback runs on the test thread.
 */
public class SnapshotsServiceIoTimeoutSeamTests extends OpenSearchTestCase {

    /**
     * An operation label of the shape a call site passes. The value is opaque to the seam -- it reaches only the log
     * line and the timeout message -- so a literal is honest here; it is not the string any call site builds. The
     * nested brackets are deliberate, since both places it lands wrap it in brackets of their own.
     */
    private static final String DESCRIPTION = "delete 2 snapshot(s) from [test-repo]";

    /** The setting's own floor: its parser rejects anything under 1s. */
    private static final TimeValue MIN_BUDGET = TimeValue.timeValueSeconds(1);

    /** Distinct from both the 30m default and {@link #MIN_BUDGET}, so a message naming it is unambiguous. */
    private static final TimeValue MID_FLIGHT_BUDGET = TimeValue.timeValueMinutes(20);

    private DeterministicTaskQueue taskQueue;
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        taskQueue = new DeterministicTaskQueue(Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build(), random());
        // Once: every call to getThreadPool builds another pool, and the queue-is-empty assertions below are only
        // meaningful while one pool is in play.
        threadPool = taskQueue.getThreadPool();
    }

    /**
     * Both halves live on one instance on purpose: the flag is read at call time, not captured at construction, so one
     * service answering differently either side of the flip is the actual claim. Two instances would let a
     * construction-time capture pass.
     * <p>
     * The final assertion after the lock closes is what makes the name's "only while" true in both directions, catching a
     * one-way latch.
     */
    public void testInstanceSeamWrapsOnlyWhileTheFeatureFlagIsEnabled() {
        assertFalse(FeatureFlags.isEnabled(FeatureFlags.SNAPSHOT_RESILIENCE_SETTING));
        // Settings.EMPTY, not a seeded budget: with the flag off the setting's validator rejects the key when it is
        // present, so a flag-off node can only ever be constructed on the default.
        final SnapshotsService service = newClusterManagerSnapshotsService(newClusterSettings(), Settings.EMPTY, threadPool);
        final ActionListener<Void> listener = ActionListener.wrap(() -> {});

        assertSame("flag off must return the caller's own listener", listener, service.withRepositoryIoTimeout(DESCRIPTION, listener));
        assertFalse("flag off must arm no timer", taskQueue.hasDeferredTasks());

        try (FeatureFlags.TestUtils.FlagWriteLock ignored = new FeatureFlags.TestUtils.FlagWriteLock(FeatureFlags.SNAPSHOT_RESILIENCE)) {
            assertNotSame("flag on must interpose a wrapper", listener, service.withRepositoryIoTimeout(DESCRIPTION, listener));
            assertTrue("flag on must arm a timer", taskQueue.hasDeferredTasks());
            // Drained rather than abandoned, so the queue is empty again and the assertion below means what it says.
            // The listener above absorbs either outcome.
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
        }

        assertSame("the flag is read per call, not latched on first use", listener, service.withRepositoryIoTimeout(DESCRIPTION, listener));
        assertFalse("and nothing may be armed once the flag is off again", taskQueue.hasDeferredTasks());
    }

    /**
     * Callers take the per-repository operation token before reaching the seam, so a rejection escaping it would leak
     * the token and wedge the repository for the life of the node -- during a shutdown, when nobody is watching. The
     * sibling file pins the same degradation on the static helper with a terminated pool; new here is that it holds
     * through the instance seam, and the warning, which is the only signal an operator gets that an operation is
     * running unbudgeted.
     * <p>
     * A stub pool rather than a terminated one, because the point here is the pinned rejection type:
     * {@code ThreadPool#schedule} does not declare it. The expectation matches the stable prefix and the operation
     * name, not the trailing clause, so rewording the advice does not redden a test whose subject is the fallback.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testRejectedScheduleLogsAWarningAndReturnsTheUnwrappedListener() throws Exception {
        final ThreadPool rejectingThreadPool = mock(ThreadPool.class);
        when(rejectingThreadPool.schedule(any(Runnable.class), any(TimeValue.class), anyString())).thenThrow(
            new OpenSearchRejectedExecutionException("rejected for test", true)
        );
        final SnapshotsService service = newClusterManagerSnapshotsService(newClusterSettings(), Settings.EMPTY, rejectingThreadPool);
        final ActionListener<Void> listener = ActionListener.wrap(() -> {});

        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(SnapshotsService.class))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "unbudgeted fallback warning",
                    SnapshotsService.class.getCanonicalName(),
                    Level.WARN,
                    "Could not schedule I/O timeout for [" + DESCRIPTION + "]"
                )
            );

            // Nothing may propagate: this call not throwing is itself one of the assertions.
            final ActionListener<Void> returned = service.withRepositoryIoTimeout(DESCRIPTION, listener);

            assertSame("a rejected schedule must degrade to the unbudgeted listener, not throw", listener, returned);
            appender.assertAllExpectationsMatched();
        }
    }

    /**
     * Drives the production expiry hook, which is the half the sibling file's equivalent test explicitly disclaims:
     * that test supplies its own hook and so never runs the one that must fail the <em>delegate</em> rather than the
     * wrapper it is handed. Failing the wrapper would reach nothing, because the wrapper has already flipped its own
     * done-flag, and the caller would wait forever.
     * <p>
     * The drop half is the single done-flag that all of {@code onResponse}, {@code onFailure} and {@code run} open by
     * compare-and-set, and it is load-bearing here: a
     * delete's failure arm removes the in-progress entry and releases the per-repository claim, so a second delivery
     * would answer the caller twice and release a claim no longer held.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testExpiryFailsTheDelegateAndALateCompletionIsDropped() {
        final SnapshotsService service = newClusterManagerSnapshotsService(
            newClusterSettings(),
            Settings.builder().put(SnapshotsService.SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING.getKey(), MIN_BUDGET.getStringRep()).build(),
            threadPool
        );

        final AtomicInteger responses = new AtomicInteger();
        final AtomicInteger failures = new AtomicInteger();
        final AtomicReference<Exception> failure = new AtomicReference<>();
        final ActionListener<Void> delegate = new ActionListener<Void>() {
            @Override
            public void onResponse(Void unused) {
                responses.incrementAndGet();
            }

            @Override
            public void onFailure(Exception e) {
                failures.incrementAndGet();
                failure.set(e);
            }
        };

        final ActionListener<Void> wrapped = service.withRepositoryIoTimeout(DESCRIPTION, delegate);
        assertNotSame(delegate, wrapped);
        assertEquals("nothing may reach the delegate before the budget expires", 0, failures.get() + responses.get());

        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();

        assertEquals("expiry must fail the delegate exactly once", 1, failures.get());
        assertThat(
            "expiry must present as a timeout, which is what a call site keys its bookkeeping on",
            failure.get(),
            instanceOf(OpenSearchTimeoutException.class)
        );
        assertThat("and must name the operation it gave up on", failure.get().getMessage(), containsString("[" + DESCRIPTION + "]"));

        // The real repository answering at last. Must be swallowed.
        wrapped.onResponse(null);

        assertEquals("a late completion must not reach the delegate", 0, responses.get());
        assertEquals("the delegate must be completed exactly once", 1, failures.get());
    }

    /**
     * The sibling file proves the mirrored field moves when the setting is updated. This proves the wrap consumes that
     * field, and consumes it once, at wrap time.
     * <p>
     * Three values separate the three implementations that differ only here. A budget frozen at construction would arm
     * the timer on the 30m default and the message would name it. A budget re-read when the timer fires would name the
     * value applied after the wrap. Only a single read at wrap time names the value that actually armed the timer, and
     * the message is a faithful witness to it because production takes one read and feeds both. An operator shortening
     * this setting mid-incident is the one moment it exists for, and either other reading makes that a no-op or a lie.
     */
    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testWrapReadsTheBudgetOnceAtWrapTimeNotAtBootAndNotAtExpiry() {
        final ClusterSettings clusterSettings = newClusterSettings();
        // Unseeded, so the node boots on the 30m default and no assertion here restates the sibling file's seeding test.
        final SnapshotsService service = newClusterManagerSnapshotsService(clusterSettings, Settings.EMPTY, threadPool);

        clusterSettings.applySettings(
            Settings.builder().put(SnapshotsService.SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING.getKey(), MIN_BUDGET.getStringRep()).build()
        );

        final AtomicInteger responses = new AtomicInteger();
        final AtomicReference<Exception> failure = new AtomicReference<>();
        service.withRepositoryIoTimeout(DESCRIPTION, ActionListener.<Void>wrap(unused -> responses.incrementAndGet(), failure::set));

        // Strictly between the wrap and the expiry, which is only orderable because the timer is on the deterministic
        // queue. A re-read when the timer fires would report this value instead of the one that armed it.
        clusterSettings.applySettings(
            Settings.builder()
                .put(SnapshotsService.SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING.getKey(), MID_FLIGHT_BUDGET.getStringRep())
                .build()
        );
        assertEquals(
            "the second update must land before the timer fires, or this proves nothing",
            MID_FLIGHT_BUDGET,
            service.repositoryIoTimeout()
        );

        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();

        assertEquals("the timer must not deliver a success", 0, responses.get());
        assertNotNull("the updated budget never governed", failure.get());
        assertThat(failure.get().getMessage(), containsString("[" + DESCRIPTION + "]"));
        assertThat(
            "the message must name the budget that armed the timer, not the boot value and not the current one",
            failure.get().getMessage(),
            containsString("timed out after [" + MIN_BUDGET + "]")
        );
    }

    private static ClusterSettings newClusterSettings() {
        return new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    }

    /**
     * Builds a live cluster-manager-role {@link SnapshotsService} over the given cluster settings: the budget seed and its
     * consumer exist only on a cluster-manager node.
     */
    private SnapshotsService newClusterManagerSnapshotsService(
        ClusterSettings clusterSettings,
        Settings nodeSettings,
        ThreadPool servicePool
    ) {
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        final TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(servicePool);

        final Settings settings = Settings.builder()
            .put(nodeSettings)
            .put("node.name", "test")
            .putList("node.roles", "cluster_manager", "data")
            .build();

        return new SnapshotsService(
            settings,
            clusterService,
            mock(IndexNameExpressionResolver.class),
            mock(RepositoriesService.class),
            transportService,
            mock(ActionFilters.class),
            null,
            new RemoteStoreSettings(Settings.EMPTY, clusterSettings),
            null
        );
    }
}
