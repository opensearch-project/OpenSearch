/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import org.opensearch.plugins.Plugin;

import java.util.Stack;

/**
 * An ExecutionContext is a singular header within ThreadLocal that contains the chain of plugins on the execution path
 */
public class ExecutionContext {
    private final ThreadLocal<Stack<String>> context = new ThreadLocal<>();

    public void add(Plugin plugin) {
        if (context.get() == null) {
            context.set(new Stack<>());
        }
        context.get().add(plugin.getClass().getCanonicalName());
    }

    public Stack<String> get() {
        if (context.get() == null) {
            return null;
        }
        return context.get();
    }

    public String peek() {
        if (context.get() == null || context.get().isEmpty()) {
            return null;
        }
        return context.get().peek();
    }

    public String pop() {
        if (context.get() == null || context.get().isEmpty()) {
            return null;
        }
        return context.get().pop();
    }
}
