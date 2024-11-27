package org.opensearch.javaagent;

import org.opensearch.javaagent.bootstrap.AgentPolicy;

import java.lang.StackWalker.Option;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.SocketPermission;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.util.List;

import net.bytebuddy.asm.Advice;
import net.bytebuddy.asm.Advice.Origin;

public class SocketChannelInterceptor {
    @Advice.OnMethodEnter
    public static void intercept(@Advice.AllArguments Object[] args, @Origin Method method) throws Exception {
        final Policy policy = AgentPolicy.getPolicy();
        System.out.println("Policy: " + policy);

        final StackWalker walker = StackWalker.getInstance(Option.RETAIN_CLASS_REFERENCE);

        final Class<?> caller = walker.getCallerClass();
        final List<ProtectionDomain> callers = walker.walk(new StackCallerChainExtractor());

        final InetSocketAddress address = (InetSocketAddress) args[0];
        for (final ProtectionDomain domain : callers) {
            if (!policy.implies(domain, new SocketPermission(address.getHostName() + ":" + address.getPort(), "connect,resolve"))) {
                throw new SecurityException("Denied access to " + address);
            }
        }
    }
}
