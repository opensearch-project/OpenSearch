/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.threadpool;

import org.opensearch.concurrent.ThreadContextProxy;
import org.opensearch.plugins.Plugin;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class ThreadPoolProxy implements InvocationHandler {
    private final ThreadPool threadPool;
    private final Plugin plugin;

    public static ThreadPool newInstance(ThreadPool threadPool, Plugin plugin) {
        return (ThreadPool) Proxy.newProxyInstance(
            threadPool.getClass().getClassLoader(),
            new Class<?>[] { ThreadPool.class },
            new ThreadPoolProxy(threadPool, plugin)
        );
    }

    private ThreadPoolProxy(ThreadPool threadPool, Plugin plugin) {
        this.threadPool = threadPool;
        this.plugin = plugin;
    }

    @Override
    public Object invoke(Object proxy, Method m, Object[] args) throws Throwable {
        Object result;
        if ("getThreadContext".equals(m.getName())) {
            result = ThreadContextProxy.getInstance(threadPool.getThreadContext(), plugin);
        } else {
            result = m.invoke(threadPool, args);
        }
        return result;
    }
}
