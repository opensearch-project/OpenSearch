/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.javaagent.bootstrap.internal;

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

    /**
     * The JDK AccessController (marked for removal), temporarily supported alongside its replacement in the Java Agent.
     */
    @SuppressWarnings("removal")
    private static final Class<?> JDK_ACCESS_CONTROLLER = java.security.AccessController.class;

    /**
     * Holder for the OpenSearch AccessController. It is not on the boot class path, so it can't be
     * referenced directly from here. The trusted copy is the one defined by the system class loader,
     * so resolve it on first use.
     */
    private static final class OpenSearchAccessController {
        static final Class<?> CLASS = load();

        private static Class<?> load() {
            try {
                return Class.forName("org.opensearch.secure_sm.AccessController", false, ClassLoader.getSystemClassLoader());
            } catch (ClassNotFoundException | LinkageError e) {
                return null;
            }
        }
    }

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
        return frames.takeWhile(frame -> isPrivilegedFrame(frame) == false)
            .map(StackFrame::getDeclaringClass)
            .map(Class::getProtectionDomain)
            .filter(pd -> pd.getCodeSource() != null) // Filter out JDK classes
            .filter(pd -> !"jrt".equals(pd.getCodeSource().getLocation().getProtocol())) // Filter out codesources beginning with jrt:
            .collect(Collectors.toSet());
    }

    /**
     * Checks whether the frame is a call to doPrivileged on a trusted AccessController.
     * @param frame stack frame
     * @return whether callers below this frame should be excluded from permission checks
     */
    private static boolean isPrivilegedFrame(StackFrame frame) {
        final Class<?> clazz = frame.getDeclaringClass();
        if (clazz != JDK_ACCESS_CONTROLLER && clazz != OpenSearchAccessController.CLASS) {
            return false;
        }
        return DO_PRIVILEGED_METHODS.contains(frame.getMethodName());
    }
}
