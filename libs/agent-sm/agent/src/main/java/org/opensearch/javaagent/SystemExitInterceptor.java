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
    public static void intercept(int code) throws Exception {
        final StackWalker walker = StackWalker.getInstance(Option.RETAIN_CLASS_REFERENCE);
        final Class<?> caller = walker.getCallerClass();

        if (!AgentPolicy.isClassThatCanExit(caller.getName())) {
            throw new SecurityException("The class " + caller + " is not allowed to call System::exit(" + code + ")");
        }
    }
}
