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
import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Stack Caller Chain Extractor
 */
public final class StackCallerProtectionDomainChainExtractor implements Function<Stream<StackFrame>, Collection<ProtectionDomain>> {
    /**
     * Single instance of stateless class.
     */
    public static final StackCallerProtectionDomainChainExtractor INSTANCE = new StackCallerProtectionDomainChainExtractor();

    /**
     * Constructor
     */
    private StackCallerProtectionDomainChainExtractor() {}

    /**
     * Folds the stack
     * @param frames stack frames
     */
    @Override
    public Collection<ProtectionDomain> apply(Stream<StackFrame> frames) {
        return frames.map(StackFrame::getDeclaringClass)
            .map(Class::getProtectionDomain)
            .filter(pd -> pd.getCodeSource() != null) /* JDK */
            .collect(Collectors.toSet());
    }
}
