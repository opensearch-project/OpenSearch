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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Agent Policy
 */
@SuppressWarnings("removal")
public class AgentPolicy {
    private static final Logger LOGGER = Logger.getLogger(AgentPolicy.class.getName());
    private static volatile Policy policy;
    private static volatile Set<String> trustedHosts;
    private static volatile Set<String> trustedFileSystems;
    private static volatile BiFunction<Class<?>, Collection<Class<?>>, Boolean> classesThatCanExit;

    /**
     * None of the classes is allowed to call {@link System#exit} or {@link Runtime#halt}
     */
    public static final class NoneCanExit implements BiFunction<Class<?>, Collection<Class<?>>, Boolean> {
        /**
         * NoneCanExit
         */
        public NoneCanExit() {}

        /**
         * Check if class is allowed to call {@link System#exit}, {@link Runtime#halt}
         * @param caller caller class
         * @param chain chain of call classes
         * @return is class allowed to call {@link System#exit}, {@link Runtime#halt} or not
         */
        @Override
        public Boolean apply(Class<?> caller, Collection<Class<?>> chain) {
            return true;
        }
    }

    /**
     * Only caller is allowed to call {@link System#exit} or {@link Runtime#halt}
     */
    public static final class CallerCanExit implements BiFunction<Class<?>, Stream<Class<?>>, Boolean> {
        private final String[] classesThatCanExit;

        /**
         * CallerCanExit
         * @param classesThatCanExit classes that can exit
         */
        public CallerCanExit(final String[] classesThatCanExit) {
            this.classesThatCanExit = classesThatCanExit;
        }

        /**
         * Check if class is allowed to call {@link System#exit}, {@link Runtime#halt}
         * @param caller caller class
         * @param chain chain of call classes
         * @return is class allowed to call {@link System#exit}, {@link Runtime#halt} or not
         */
        @Override
        public Boolean apply(Class<?> caller, Stream<Class<?>> chain) {
            for (final String classThatCanExit : classesThatCanExit) {
                if (caller.getName().equalsIgnoreCase(classThatCanExit)) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Any caller in the chain is allowed to call {@link System#exit} or {@link Runtime#halt}
     */
    public static final class AnyCanExit implements BiFunction<Class<?>, Collection<Class<?>>, Boolean> {
        private final String[] classesThatCanExit;

        /**
         * AnyCanExit
         * @param classesThatCanExit classes that can exit
         */
        public AnyCanExit(final String[] classesThatCanExit) {
            this.classesThatCanExit = classesThatCanExit;
        }

        /**
         * Check if class is allowed to call {@link System#exit}, {@link Runtime#halt}
         * @param caller caller class
         * @param chain chain of call classes
         * @return is class allowed to call {@link System#exit}, {@link Runtime#halt} or not
         */
        @Override
        public Boolean apply(Class<?> caller, Collection<Class<?>> chain) {
            for (final Class<?> clazz : chain) {
                for (final String classThatCanExit : classesThatCanExit) {
                    if (clazz.getName().matches(classThatCanExit)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    private AgentPolicy() {}

    /**
     * Set Agent policy
     * @param policy policy
     */
    public static void setPolicy(Policy policy) {
        setPolicy(policy, Set.of(), Set.of(), new NoneCanExit());
    }

    /**
     * Set Agent policy
     * @param policy policy
     * @param trustedHosts trusted hosts
     * @param trustedFileSystems trusted file systems
     * @param classesThatCanExit classed that are allowed to call {@link System#exit}, {@link Runtime#halt}
     */
    public static void setPolicy(
        Policy policy,
        final Set<String> trustedHosts,
        final Set<String> trustedFileSystems,
        final BiFunction<Class<?>, Collection<Class<?>>, Boolean> classesThatCanExit
    ) {
        if (AgentPolicy.policy == null) {
            AgentPolicy.policy = policy;
            AgentPolicy.trustedHosts = Collections.unmodifiableSet(trustedHosts);
            AgentPolicy.trustedFileSystems = Collections.unmodifiableSet(trustedFileSystems);
            AgentPolicy.classesThatCanExit = classesThatCanExit;
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
     * Check if file system is trusted
     * @param fileSystem file system
     * @return is trusted or not
     */
    public static boolean isTrustedFileSystem(String fileSystem) {
        return AgentPolicy.trustedFileSystems.contains(fileSystem);
    }

    /**
     * Check if class is allowed to call {@link System#exit}, {@link Runtime#halt}
     * @param caller caller class
     * @param chain chain of call classes
     * @return is class allowed to call {@link System#exit}, {@link Runtime#halt} or not
     */
    public static boolean isChainThatCanExit(Class<?> caller, Collection<Class<?>> chain) {
        return classesThatCanExit.apply(caller, chain);
    }
}
