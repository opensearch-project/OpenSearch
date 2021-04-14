/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.master;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.opensearch.cluster.service.MasterTaskThrottlingException;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.transport.TransportException;

import java.util.Iterator;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * ActionListener for retrying the Throttled master tasks.
 * It schedules the retry on the Throttling Exception from master node and
 * delegates the response if it receive response from master.
 *
 * It uses ExponentialEqualJitterBackoff policy for determining delay between retries.
 */
public class MasterThrottlingRetryListener<Request extends MasterNodeRequest<Request>, Response extends ActionResponse>
        implements ActionListener<Response>  {

    private static final Logger logger = LogManager.getLogger(MasterThrottlingRetryListener.class);

    /**
     * Base delay in millis.
     */
    private final int BASE_DELAY_MILLIS = 10;

    /**
     * Maximum delay in millis.
     */
    private final int MAX_DELAY_MILLIS = 5000;

    private long totalDelay;
    private final Iterator<TimeValue> backoffDelay;
    private final ActionListener<Response> listener;
    private final Request request;
    private final Runnable runnable;
    private final String actionName;
    private final boolean localNodeRequest;

    private static ScheduledThreadPoolExecutor scheduler = Scheduler.initScheduler(Settings.EMPTY);

    public MasterThrottlingRetryListener(
            String actionName,
            Request request,
            Runnable runnable,
            ActionListener<Response> actionListener) {
        this.actionName = actionName;
        this.listener = actionListener;
        this.request = request;
        this.runnable = runnable;
        this.backoffDelay = BackoffPolicy.exponentialEqualJitterBackoff(BASE_DELAY_MILLIS, MAX_DELAY_MILLIS).iterator();
        /**
         This is to determine whether request is generated from local node or from remote node.
         If it is local node's request we need to perform the retries on this node.
         If it is remote node's request, we will not perform retries on this node and let remote node perform the retries.

         If request is from remote data node, then data node will set remoteRequest flag in {@link MasterNodeRequest}
         and send request to master, using that on master node we can determine if the request was localRequest or remoteRequest.
         */
        this.localNodeRequest = !(request.isRemoteRequest());
    }

    @Override
    public void onResponse(Response response) {
        listener.onResponse(response);
    }

    @Override
    public void onFailure(Exception e) {

        if(localNodeRequest && isThrottlingException(e)) {
            logger.info("Retrying [{}] on throttling exception from master. Error: [{}]",
                                            actionName, getExceptionMessage(e));
            long delay = backoffDelay.next().getMillis();
            if(totalDelay + delay >= request.masterNodeTimeout.getMillis()) {
                delay = request.masterNodeTimeout.getMillis() - totalDelay;
                scheduler.schedule(new Runnable() {
                    @Override
                    public void run() {
                        listener.onFailure(new ProcessClusterEventTimeoutException(request.masterNodeTimeout, actionName));
                    }
                }, delay, TimeUnit.MILLISECONDS);
            } else {
                scheduler.schedule(runnable, delay, TimeUnit.MILLISECONDS);
            }
            totalDelay += delay;
        } else {
            listener.onFailure(e);
        }
    }

    /**
     * For Testcase purposes.
     * @param retrySceduler scheduler defined in test cases.
     */
    public static void setThrottlingRetryScheduler(ScheduledThreadPoolExecutor retrySceduler) {
        scheduler = retrySceduler;
    }

    private boolean isThrottlingException(Exception e) {
        if(e instanceof TransportException) {
            return  ((TransportException)e).unwrapCause() instanceof MasterTaskThrottlingException;
        }
        return e instanceof MasterTaskThrottlingException;
    }

    private String getExceptionMessage(Exception e) {
        if(e instanceof TransportException) {
            return ((TransportException)e).unwrapCause().getMessage();
        } else {
            return e.getMessage();
        }
    }

    public static long getRetryingTasksCount() {
        return scheduler.getActiveCount() + scheduler.getQueue().size();
    }
}
