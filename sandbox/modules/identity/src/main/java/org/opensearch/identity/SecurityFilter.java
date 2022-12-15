/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.concurrent.ThreadContext.StoredContext;
import org.opensearch.rest.RestStatus;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;

public class SecurityFilter implements ActionFilter {

    protected final Logger log = LogManager.getLogger(this.getClass());
    private final ThreadContext threadContext;
    private final ClusterService cs;
    private final Client client;

    public SecurityFilter(final Client client, final Settings settings, ThreadPool threadPool, ClusterService cs) {
        this.client = client;
        this.threadContext = threadPool.getThreadContext();
        this.cs = cs;
    }

    @Override
    public int order() {
        return Integer.MIN_VALUE;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        final String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        try (StoredContext ctx = threadContext.newStoredContext(true)) {
            org.apache.logging.log4j.ThreadContext.clearAll();
            apply0(task, action, request, listener, chain);
        }
    }

    private <Request extends ActionRequest, Response extends ActionResponse> void apply0(
        Task task,
        final String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        try {
            // TODO Get jwt here and verify
            // TODO Move this logic to right after successful login
            if (threadContext.getHeader(ThreadContextConstants.OPENSEARCH_AUTHENTICATION_TOKEN_HEADER) != null) {
                String encodedJwt = threadContext.getHeader(ThreadContextConstants.OPENSEARCH_AUTHENTICATION_TOKEN_HEADER);
                String prefix = "(nodeName="
                    + cs.localNode().getId()
                    + ", requestId="
                    + request.getParentTask().getId()
                    + ", action="
                    + action
                    + " apply0)";
                log.debug(prefix + " Access token provided " + encodedJwt);
            } else {
                // TODO Figure out where internal actions are invoked and create token on invocation
                // No token provided, may be an internal request
                // Token in ThreadContext is created on REST layer and passed to Transport Layer.
                String prefix = "(nodeName="
                    + cs.localNode().getName()
                    + ", requestId="
                    + request.getParentTask().getId()
                    + ", action="
                    + action
                    + " apply0)";
                log.info(prefix + "No authorization provided in the request, internal request");
                // String err = "Access token not provided";
                // listener.onFailure(new OpenSearchSecurityException(err, RestStatus.FORBIDDEN));
            }

            final PrivilegesEvaluatorResponse pres = new PrivilegesEvaluatorResponse(); // eval.evaluate(user, action, request, task,
                                                                                        // injectedRoles);
            pres.allowed = true;

            if (log.isDebugEnabled()) {
                log.debug(pres.toString());
            }

            if (pres.isAllowed()) {
                // auditLog.logGrantedPrivileges(action, request, task);
                // auditLog.logIndexEvent(action, request, task);
                log.info("Permission granted");
                chain.proceed(task, action, request, listener);
            } else {
                // auditLog.logMissingPrivileges(action, request, task);
                String err = "Permission denied";
                log.debug(err);
                listener.onFailure(new OpenSearchSecurityException(err, RestStatus.FORBIDDEN));
            }
        } catch (OpenSearchException e) {
            if (task != null) {
                log.debug(
                    "Failed to apply filter. Task id: {} ({}). Action: {}. Error: {}",
                    task.getId(),
                    task.getDescription(),
                    action,
                    e
                );
            } else {
                log.debug("Failed to apply filter. Action: {}. Error: {}", action, e);
            }
            listener.onFailure(e);
        } catch (Throwable e) {
            log.error("Unexpected exception " + e, e);
            listener.onFailure(new OpenSearchSecurityException("Unexpected exception " + action, RestStatus.INTERNAL_SERVER_ERROR));
        }
    }
}
