/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.threadpool.ThreadPool;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class ActionPluginProxy implements InvocationHandler {
    private final ActionPlugin actionPlugin;
    private final ThreadPool threadPool;

    public static ActionPlugin newInstance(ActionPlugin obj, ThreadPool threadPool) {
        return (ActionPlugin) Proxy.newProxyInstance(
            obj.getClass().getClassLoader(),
            new Class<?>[] { ActionPlugin.class },
            new ActionPluginProxy(obj, threadPool)
        );
    }

    private ActionPluginProxy(ActionPlugin actionPlugin, ThreadPool threadPool) {
        this.actionPlugin = actionPlugin;
        this.threadPool = threadPool;
    }

    @Override
    public Object invoke(Object proxy, Method m, Object[] args) throws Throwable {
        Object result;
        try {
            threadPool.getThreadContext().setExecutionContext(((Plugin) actionPlugin).getClass().getName());
            result = m.invoke(actionPlugin, args);
            threadPool.getThreadContext().clearExecutionContext();
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        } catch (Exception e) {
            throw new RuntimeException("unexpected invocation exception: " + e.getMessage());
        }
        return result;
    }
}
