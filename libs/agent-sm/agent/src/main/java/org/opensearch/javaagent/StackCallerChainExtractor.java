/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.javaagent;

import java.lang.StackWalker.StackFrame;
import java.security.ProtectionDomain;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Stack Caller Chain Extractor
 */
public final class StackCallerChainExtractor implements Function<Stream<StackFrame>, Stream<ProtectionDomain>> {
    /**
     * Single instance of stateless class.
     */
    public static final StackCallerChainExtractor INSTANCE = new StackCallerChainExtractor();

    /**
     * Constructor
     */
    private StackCallerChainExtractor() {}

    /**
     * Folds the stack
     * @param frames stack frames
     */
    @Override
    public Stream<ProtectionDomain> apply(Stream<StackFrame> frames) {
        return frames.map(StackFrame::getDeclaringClass)
            .map(Class::getProtectionDomain)
            .filter(pd -> pd.getCodeSource() != null) /* JDK */
            .distinct();
    }
}
