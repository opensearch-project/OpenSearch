/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.javaagent.bootstrap;

import java.lang.StackWalker.Option;
import java.lang.StackWalker.StackFrame;
import java.security.Permission;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Agent Policy
 */
@SuppressWarnings("removal")
public class AgentPolicy {
    private static final Logger LOGGER = Logger.getLogger(AgentPolicy.class.getName());
    private static volatile Policy policy;
    private static volatile Set<String> trustedHosts;
    private static volatile Set<String> classesThatCanExit;

    private AgentPolicy() {}

    /**
     * Set Agent policy
     * @param policy policy
     */
    public static void setPolicy(Policy policy) {
        setPolicy(policy, Set.of(), new String[0]);
    }

    /**
     * Set Agent policy
     * @param policy policy
     * @param trustedHosts trusted hosts
     * @param classesThatCanExit classed that are allowed to call {@link System#exit}
     */
    public static void setPolicy(Policy policy, final Set<String> trustedHosts, final String[] classesThatCanExit) {
        if (AgentPolicy.policy == null) {
            AgentPolicy.policy = policy;
            AgentPolicy.trustedHosts = Collections.unmodifiableSet(trustedHosts);
            AgentPolicy.classesThatCanExit = Arrays.stream(classesThatCanExit).collect(Collectors.toSet());
            LOGGER.info("Policy attached successfully: " + policy);
        } else {
            throw new SecurityException("The Policy has been set already: " + AgentPolicy.policy);
        }
    }

    /**
     * Check permissions
     * @param permission permission
     */
    public static void checkPermission(Permission permission) {
        final StackWalker walker = StackWalker.getInstance(Option.RETAIN_CLASS_REFERENCE);
        final List<ProtectionDomain> callers = walker.walk(
            frames -> frames.map(StackFrame::getDeclaringClass).map(Class::getProtectionDomain).distinct().collect(Collectors.toList())
        );

        for (final ProtectionDomain domain : callers) {
            if (!policy.implies(domain, permission)) {
                throw new SecurityException("Denied access: " + permission);
            }
        }
    }

    /**
     * Get policy
     * @return policy
     */
    public static Policy getPolicy() {
        return policy;
    }

    /**
     * Check if hostname is trusted
     * @param hostname hostname
     * @return is trusted or not
     */
    public static boolean isTrustedHost(String hostname) {
        return AgentPolicy.trustedHosts.contains(hostname);
    }

    /**
     * Check if class is allowed to call {@link System#exit}
     * @param name class name
     * @return is class allowed to call {@link System#exit} or not
     */
    public static boolean isClassThatCanExit(String name) {
        return AgentPolicy.classesThatCanExit.contains(name);
    }
}
