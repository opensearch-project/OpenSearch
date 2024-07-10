/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.threadpool.ThreadPool;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * RestHandlerProxy is a wrapper around {@link RestHandler} that populates the ExecutionContext prior
 * to delegating execution to a plugin for handling a REST Request
 */
public class RestHandlerProxy implements InvocationHandler {
    private final RestHandler restHandler;
    private final ThreadPool threadPool;
    private final ActionPlugin plugin;

    public static RestHandler newInstance(RestHandler obj, ThreadPool threadPool, ActionPlugin plugin) {
        return (RestHandler) Proxy.newProxyInstance(
            obj.getClass().getClassLoader(),
            new Class<?>[] { RestHandler.class },
            new RestHandlerProxy(obj, threadPool, plugin)
        );
    }

    private RestHandlerProxy(RestHandler restHandler, ThreadPool threadPool, ActionPlugin plugin) {
        this.restHandler = restHandler;
        this.threadPool = threadPool;
        this.plugin = plugin;
    }

    @Override
    public Object invoke(Object proxy, Method m, Object[] args) throws Throwable {
        Object result;
        try (ThreadContext.StoredContext threadContext = threadPool.getThreadContext().switchContext((Plugin) plugin)) {
            result = m.invoke(restHandler, args);
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        } catch (Exception e) {
            throw new RuntimeException("unexpected invocation exception: " + e.getMessage());
        }
        return result;
    }
}
