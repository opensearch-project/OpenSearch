/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.javaagent;

import java.lang.StackWalker.StackFrame;
import java.util.Collection;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Stack Caller Class Chain Extractor
 */
public final class StackCallerClassChainExtractor implements Function<Stream<StackFrame>, Collection<Class<?>>> {
    /**
     * Single instance of stateless class.
     */
    public static final StackCallerClassChainExtractor INSTANCE = new StackCallerClassChainExtractor();

    /**
     * Constructor
     */
    private StackCallerClassChainExtractor() {}

    /**
     * Folds the stack
     * @param frames stack frames
     */
    @Override
    public Collection<Class<?>> apply(Stream<StackFrame> frames) {
        return cast(frames);
    }

    @SuppressWarnings("unchecked")
    private static <A> Set<A> cast(Stream<StackFrame> frames) {
        return (Set<A>) frames.map(StackFrame::getDeclaringClass).filter(c -> !c.isHidden()).collect(Collectors.toSet());
    }
}
