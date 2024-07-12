/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.concurrent;

import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.plugins.Plugin;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

public class ThreadContextProxy implements InvocationHandler {
    private final ThreadContext threadContext;
    private final Plugin plugin;
    private static final Map<String, ThreadContext> INSTANCES = new HashMap<>();

    public static ThreadContext getInstance(ThreadContext threadContext, Plugin plugin) {
        if (!INSTANCES.containsKey(plugin.getClass().getCanonicalName())) {
            ThreadContext threadContextProxy = (ThreadContext) Proxy.newProxyInstance(
                threadContext.getClass().getClassLoader(),
                new Class<?>[] { ThreadContext.class },
                new ThreadContextProxy(threadContext, plugin)
            );
            INSTANCES.put(plugin.getClass().getCanonicalName(), threadContextProxy);
        }
        return INSTANCES.get(plugin.getClass().getCanonicalName());
    }

    private ThreadContextProxy(ThreadContext threadContext, Plugin plugin) {
        this.threadContext = threadContext;
        this.plugin = plugin;
    }

    @Override
    public Object invoke(Object proxy, Method m, Object[] args) throws Throwable {
        Object result;
        if ("stashContext".equals(m.getName())) {
            result = m.invoke(threadContext, plugin.getClass(), args);
        } else {
            result = m.invoke(threadContext, args);
        }
        return result;
    }
}
