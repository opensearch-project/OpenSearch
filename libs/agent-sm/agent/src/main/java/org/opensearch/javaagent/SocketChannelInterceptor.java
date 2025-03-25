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
import java.lang.StackWalker.StackFrame;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.NetPermission;
import java.net.SocketPermission;
import java.net.UnixDomainSocketAddress;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.util.HashSet;
import java.util.Set;

import net.bytebuddy.asm.Advice;
import net.bytebuddy.asm.Advice.Origin;

/**
 * SocketChannelInterceptor
 */
public class SocketChannelInterceptor {
    private static final StackWalker walker = StackWalker.getInstance(Option.RETAIN_CLASS_REFERENCE);

    /**
     * SocketChannelInterceptor
     */
    public SocketChannelInterceptor() {}

    /**
     * Interceptors
     * @param args arguments
     * @param method method
     * @throws Exception exceptions
     */
    @Advice.OnMethodEnter
    @SuppressWarnings("removal")
    public static void intercept(@Advice.AllArguments Object[] args, @Origin Method method) throws Exception {
        final Policy policy = AgentPolicy.getPolicy();
        if (policy == null) {
            return; /* noop */
        }

        java.security.Permission permission = null;

        if (args[0] instanceof InetSocketAddress address) {
            if (!AgentPolicy.isTrustedHost(address.getHostString())) {
                final String host = address.getHostString() + ":" + address.getPort();
                permission = new SocketPermission(host, "connect,resolve");
            }
        } else if (args[0] instanceof UnixDomainSocketAddress) {
            permission = new NetPermission("accessUnixDomainSocket");
        }

        if (permission != null) {
            checkPermission(policy, permission);
        }
    }

    /**
    * permission evauations
    * @param policy policy
    * @param permission permission
    * @throws Exception exceptions
    */
    @SuppressWarnings("removal")
    private static void checkPermission(Policy policy, java.security.Permission permission) throws Exception {
        Set<ProtectionDomain> visitedDomains = new HashSet<>();

        walker.walk(frames -> {
            frames.map(StackFrame::getDeclaringClass)
                .map(Class::getProtectionDomain)
                .filter(pd -> pd.getCodeSource() != null)
                .filter(visitedDomains::add)  // Only process domains we haven't seen yet in the current walk
                .forEach(pd -> {
                    if (!policy.implies(pd, permission)) {
                        throw new SecurityException("Denied access, domain " + pd);
                    }
                });
            return null; // Return value not used
        });
    }
}
