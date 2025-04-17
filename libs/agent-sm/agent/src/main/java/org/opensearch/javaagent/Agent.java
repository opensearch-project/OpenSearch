/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.javaagent;

import org.opensearch.javaagent.bootstrap.AgentPolicy;

import java.lang.instrument.Instrumentation;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.spi.FileSystemProvider;
import java.util.Map;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.loading.ClassInjector;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.matcher.ElementMatcher.Junction;
import net.bytebuddy.matcher.ElementMatchers;

/**
 * Java Agent
 */
public class Agent {
    /**
     * Constructor
     */
    private Agent() {}

    /**
     * List of methods that are intercepted
     */
    private static final String[] INTERCEPTED_METHODS = {
        "write",
        "createFile",
        "createDirectories",
        "createLink",
        "copy",
        "move",
        "newByteChannel",
        "delete",
        "deleteIfExists",
        "read",
        "open" };

    /**
     * Premain
     * @param agentArguments agent arguments
     * @param instrumentation instrumentation
     * @throws Exception Exception
     */
    public static void premain(String agentArguments, Instrumentation instrumentation) throws Exception {
        initAgent(instrumentation);
    }

    /**
     * Agent Main
     * @param agentArguments agent arguments
     * @param instrumentation instrumentation
     * @throws Exception Exception
     */
    public static void agentmain(String agentArguments, Instrumentation instrumentation) throws Exception {
        initAgent(instrumentation);
    }

    private static AgentBuilder createAgentBuilder(Instrumentation inst) throws Exception {
        final Junction<TypeDescription> systemType = ElementMatchers.isSubTypeOf(SocketChannel.class);
        final Junction<TypeDescription> pathType = ElementMatchers.isSubTypeOf(Files.class);
        final Junction<TypeDescription> fileChannelType = ElementMatchers.isSubTypeOf(FileChannel.class);
        final Junction<TypeDescription> fileSystemProviderType = ElementMatchers.isSubTypeOf(FileSystemProvider.class);

        final AgentBuilder.Transformer socketTransformer = (b, typeDescription, classLoader, module, pd) -> b.visit(
            Advice.to(SocketChannelInterceptor.class)
                .on(ElementMatchers.named("connect").and(ElementMatchers.not(ElementMatchers.isAbstract())))
        );

        final AgentBuilder.Transformer fileTransformer = (b, typeDescription, classLoader, module, pd) -> b.visit(
            Advice.to(FileInterceptor.class).on(ElementMatchers.namedOneOf(INTERCEPTED_METHODS).or(ElementMatchers.isAbstract()))
        );

        ClassInjector.UsingUnsafe.ofBootLoader()
            .inject(
                Map.of(
                    new TypeDescription.ForLoadedType(StackCallerProtectionDomainChainExtractor.class),
                    ClassFileLocator.ForClassLoader.read(StackCallerProtectionDomainChainExtractor.class),
                    new TypeDescription.ForLoadedType(StackCallerClassChainExtractor.class),
                    ClassFileLocator.ForClassLoader.read(StackCallerClassChainExtractor.class),
                    new TypeDescription.ForLoadedType(AgentPolicy.class),
                    ClassFileLocator.ForClassLoader.read(AgentPolicy.class)
                )
            );

        final ByteBuddy byteBuddy = new ByteBuddy().with(Implementation.Context.Disabled.Factory.INSTANCE);
        final AgentBuilder agentBuilder = new AgentBuilder.Default(byteBuddy).with(AgentBuilder.InitializationStrategy.NoOp.INSTANCE)
            .with(AgentBuilder.RedefinitionStrategy.REDEFINITION)
            .with(AgentBuilder.TypeStrategy.Default.REDEFINE)
            .ignore(ElementMatchers.nameContains("$MockitoMock$")) /* ingore all Mockito mocks */
            .type(systemType)
            .transform(socketTransformer)
            .type(pathType.or(fileChannelType).or(fileSystemProviderType))
            .transform(fileTransformer)
            .type(ElementMatchers.is(java.lang.System.class))
            .transform(
                (b, typeDescription, classLoader, module, pd) -> b.visit(
                    Advice.to(SystemExitInterceptor.class).on(ElementMatchers.named("exit"))
                )
            )
            .type(ElementMatchers.is(java.lang.Runtime.class))
            .transform(
                (b, typeDescription, classLoader, module, pd) -> b.visit(
                    Advice.to(RuntimeHaltInterceptor.class).on(ElementMatchers.named("halt"))
                )
            );

        return agentBuilder;
    }

    private static void initAgent(Instrumentation instrumentation) throws Exception {
        AgentBuilder agentBuilder = createAgentBuilder(instrumentation);
        agentBuilder.installOn(instrumentation);
    }
}
