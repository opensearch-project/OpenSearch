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
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.NetPermission;
import java.net.SocketPermission;
import java.net.UnixDomainSocketAddress;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.util.List;

import net.bytebuddy.asm.Advice;
import net.bytebuddy.asm.Advice.Origin;

/**
 * SocketChannelInterceptor
 */
public class SocketChannelInterceptor {
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

        final StackWalker walker = StackWalker.getInstance(Option.RETAIN_CLASS_REFERENCE);
        final List<ProtectionDomain> callers = walker.walk(new StackCallerChainExtractor());

        if (args[0] instanceof InetSocketAddress address) {
            if (!AgentPolicy.isTrustedHost(address.getHostString())) {
                final String host = address.getHostString() + ":" + address.getPort();

                final SocketPermission permission = new SocketPermission(host, "connect,resolve");
                for (final ProtectionDomain domain : callers) {
                    if (!policy.implies(domain, permission)) {
                        throw new SecurityException("Denied access to: " + host + ", domain " + domain);
                    }
                }
            }
        } else if (args[0] instanceof UnixDomainSocketAddress address) {
            final NetPermission permission = new NetPermission("accessUnixDomainSocket");
            for (final ProtectionDomain domain : callers) {
                if (!policy.implies(domain, permission)) {
                    throw new SecurityException("Denied access to: " + address + ", domain " + domain);
                }
            }
        }
    }
}
