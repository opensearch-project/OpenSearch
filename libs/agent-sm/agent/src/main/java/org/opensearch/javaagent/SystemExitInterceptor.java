/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.javaagent;

import org.opensearch.javaagent.bootstrap.AgentPolicy;

import java.lang.StackWalker.Option;
import java.security.Policy;
import java.util.Collection;

import net.bytebuddy.asm.Advice;

/**
 * {@link System#exit} interceptor
 */
public class SystemExitInterceptor {
    /**
     * SystemExitInterceptor
     */
    public SystemExitInterceptor() {}

    /**
     * Interceptor
     * @param code exit code
     * @throws Exception exceptions
     */
    @Advice.OnMethodEnter()
    @SuppressWarnings("removal")
    public static void intercept(int code) throws Exception {
        final Policy policy = AgentPolicy.getPolicy();
        if (policy == null) {
            return; /* noop */
        }

        final StackWalker walker = StackWalker.getInstance(Option.RETAIN_CLASS_REFERENCE);
        final Class<?> caller = walker.getCallerClass();
        final Collection<Class<?>> chain = walker.walk(StackCallerClassChainExtractor.INSTANCE);

        if (AgentPolicy.isChainThatCanExit(caller, chain) == false) {
            throw new SecurityException("The class " + caller + " is not allowed to call System::exit(" + code + ")");
        }
    }
}
