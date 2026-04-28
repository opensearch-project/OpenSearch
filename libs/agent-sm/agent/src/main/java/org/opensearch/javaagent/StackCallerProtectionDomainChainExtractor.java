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
import java.util.Set;
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

    private static final StackWalker STACK_WALKER = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);
    /**
     * Classes that are used to check if the stack frame is from AccessController. Temporarily supports both the
     * AccessController from the JDK (marked for removal) and its replacement in the Java Agent.
     */
    private static final Set<String> ACCESS_CONTROLLER_CLASSES = Set.of(
        "java.security.AccessController",
        "org.opensearch.secure_sm.AccessController"
    );

    private static final Set<String> DO_PRIVILEGED_METHODS = Set.of("doPrivileged", "doPrivilegedChecked");

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
        return frames.takeWhile(
            frame -> !(ACCESS_CONTROLLER_CLASSES.contains(frame.getClassName()) && DO_PRIVILEGED_METHODS.contains(frame.getMethodName()))
        )
            .map(StackFrame::getDeclaringClass)
            .map(Class::getProtectionDomain)
            .filter(pd -> pd.getCodeSource() != null) // Filter out JDK classes
            .filter(pd -> !"jrt".equals(pd.getCodeSource().getLocation().getProtocol())) // Filter out codesources beginning with jrt:
            .collect(Collectors.toSet());
    }
}
