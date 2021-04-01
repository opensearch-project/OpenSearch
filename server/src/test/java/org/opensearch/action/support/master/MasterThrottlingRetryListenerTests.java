/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.master;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.opensearch.cluster.service.MasterTaskThrottlingException;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;

import java.time.Instant;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.AfterClass;
import org.junit.Before;

/**
 * Test class of {@link MasterThrottlingRetryListener}
 */
public class MasterThrottlingRetryListenerTests extends OpenSearchTestCase {
    private static ScheduledThreadPoolExecutor throttlingRetryScheduler = Scheduler.initScheduler(Settings.EMPTY);

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MasterThrottlingRetryListener.setThrottlingRetryScheduler(throttlingRetryScheduler);
    }

    @AfterClass
    public static void afterClass() {
        Scheduler.terminate(throttlingRetryScheduler, 30, TimeUnit.SECONDS);
    }

    public void testRetryForLocalRequest() throws BrokenBarrierException, InterruptedException {
        TransportMasterNodeActionTests.Request request = new TransportMasterNodeActionTests.Request();
        PlainActionFuture<TransportMasterNodeActionTests.Response> listener = new PlainActionFuture<>();
        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicBoolean callBackExecuted = new AtomicBoolean();
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    callBackExecuted.set(true);
                    barrier.await();
                } catch (Exception e) {
                    new AssertionError();
                }
            }
        };

        ActionListener taskRetryListener =
                new MasterThrottlingRetryListener("test", request, runnable, listener);

        taskRetryListener.onFailure(new MasterTaskThrottlingException("Throttling Exception : Limit exceeded for test"));
        barrier.await();
        assertTrue(callBackExecuted.get());
    }

    public void testRetryForRemoteRequest() throws BrokenBarrierException, InterruptedException {
        TransportMasterNodeActionTests.Request request = new TransportMasterNodeActionTests.Request();
        request.setRemoteRequest(true);
        PlainActionFuture<TransportMasterNodeActionTests.Response> listener = new PlainActionFuture<>();
        AtomicBoolean callBackExecuted = new AtomicBoolean(false);
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    callBackExecuted.set(true);
                } catch (Exception e) {
                    new AssertionError();
                }
            }
        };

        ActionListener taskRetryListener =
                new MasterThrottlingRetryListener("test", request, runnable, listener);

        taskRetryListener.onFailure(new MasterTaskThrottlingException("Throttling Exception : Limit exceeded for test"));
        Thread.sleep(100); // some buffer time so callback can execute.
        assertFalse(callBackExecuted.get());
    }

    public void testTimedOut() throws BrokenBarrierException, InterruptedException {

        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicBoolean onFailureExecuted = new AtomicBoolean();
        AtomicBoolean retryExecuted = new AtomicBoolean();
        AtomicBoolean firstExecute = new AtomicBoolean(true);
        int timeOutSec = randomIntBetween(1, 5);
        final Instant[] startTime = new Instant[1];
        final Instant[] endTime = new Instant[1];

        ActionListener listener = new ActionListener() {
            @Override
            public void onResponse(Object o) {
                new AssertionError();
            }

            @Override
            public void onFailure(Exception e) {
                endTime[0] = Instant.now();
                try {
                    onFailureExecuted.set(true);
                    barrier.await();
                } catch (Exception exe) {
                    new AssertionError();
                }
                assertEquals(ProcessClusterEventTimeoutException.class, e.getClass());
            }
        };
        TransportMasterNodeActionTests.Request request =
                new TransportMasterNodeActionTests.Request().masterNodeTimeout(TimeValue.timeValueSeconds(timeOutSec));

        class TestRetryClass {
            ActionListener listener;
            TestRetryClass(ActionListener listener) {
                this.listener = new MasterThrottlingRetryListener("test", request, this::execute, listener);
            }
            public void execute() {
                if(firstExecute.getAndSet(false)) {
                    startTime[0] = Instant.now();
                }
                listener.onFailure(new MasterTaskThrottlingException("Throttling Exception : Limit exceeded for test"));
            }
        }

        TestRetryClass testRetryClass = new TestRetryClass(listener);
        testRetryClass.execute();

        barrier.await();
        assertEquals(timeOutSec, (endTime[0].toEpochMilli() - startTime[0].toEpochMilli())/1000);
        assertTrue(onFailureExecuted.get());
        assertFalse(retryExecuted.get());
    }

    public void testRetryForDifferentException() {

        TransportMasterNodeActionTests.Request request = new TransportMasterNodeActionTests.Request();
        PlainActionFuture<TransportMasterNodeActionTests.Response> listener = new PlainActionFuture<>();
        AtomicBoolean callBackExecuted = new AtomicBoolean();
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    callBackExecuted.set(true);
                } catch (Exception e) {
                    new AssertionError();
                }
            }
        };

        ActionListener taskRetryListener =
                new MasterThrottlingRetryListener("test", request, runnable, listener);

        taskRetryListener.onFailure(new Exception());
        assertFalse(callBackExecuted.get());

        taskRetryListener.onFailure(new OpenSearchRejectedExecutionException("Different Exception"));
        assertFalse(callBackExecuted.get());
    }
}
