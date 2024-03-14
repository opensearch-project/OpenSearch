/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mockito.plugin;

import org.opensearch.common.SuppressForbidden;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.DomainCombiner;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;

import org.mockito.Incubating;
import org.mockito.MockedConstruction;
import org.mockito.internal.creation.bytebuddy.ByteBuddyMockMaker;
import org.mockito.internal.util.reflection.LenientCopyTool;
import org.mockito.invocation.MockHandler;
import org.mockito.mock.MockCreationSettings;
import org.mockito.plugins.MockMaker;

/**
 * Mockito plugin which wraps the Mockito calls into priviledged execution blocks and respects
 * SecurityManager presence.
 */
@SuppressWarnings("removal")
@SuppressForbidden(reason = "allow URL#getFile() to be used in tests")
public class PriviledgedMockMaker implements MockMaker {
    private static AccessControlContext context;
    private final ByteBuddyMockMaker delegate;

    /**
     * Create dedicated AccessControlContext to use the Mockito protection domain (test only)
     * so to relax the security constraints for the test cases which rely on mocks. This plugin
     * wraps the mock/spy creation into priviledged action using the custom access control context
     * since Mockito does not support SecurityManager out of the box. The method has to be called by
     * test framework before the SecurityManager is being set, otherwise additional permissions have
     * to be granted to the caller:
     * <p>
     *     permission java.security.Permission "createAccessControlContext"
     *
     */
    public static void createAccessControlContext() {
        // This combiner, if bound to an access control context, will unconditionally
        // substitute the call chain protection domains with the 'mockito-core' one if it
        // is present. The security checks are relaxed intentionally to trust mocking
        // implementation if it is part of the call chain.
        final DomainCombiner combiner = (current, assigned) -> Arrays.stream(current)
            .filter(pd -> pd.getCodeSource().getLocation().getFile().contains("mockito-core") /* check mockito-core only */)
            .findAny()
            .map(pd -> new ProtectionDomain[] { pd })
            .orElse(current);

        // Bind combiner to an access control context (the combiner stateless and shareable)
        final AccessControlContext wrapper = new AccessControlContext(AccessController.getContext(), combiner);

        // Create new access control context with dedicated combiner
        context = AccessController.doPrivileged((PrivilegedAction<AccessControlContext>) AccessController::getContext, wrapper);
    }

    /**
     * Construct an instance of the priviledged mock maker using ByteBuddyMockMaker under the hood.
     */
    public PriviledgedMockMaker() {
        delegate = AccessController.doPrivileged((PrivilegedAction<ByteBuddyMockMaker>) () -> new ByteBuddyMockMaker(), context);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public <T> T createMock(MockCreationSettings<T> settings, MockHandler handler) {
        return AccessController.doPrivileged((PrivilegedAction<T>) () -> delegate.createMock(settings, handler), context);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public <T> Optional<T> createSpy(MockCreationSettings<T> settings, MockHandler handler, T object) {
        // The ByteBuddyMockMaker does not implement createSpy and relies on Mockito's fallback
        return AccessController.doPrivileged((PrivilegedAction<Optional<T>>) () -> {
            T instance = delegate.createMock(settings, handler);
            new LenientCopyTool().copyToMock(object, instance);
            return Optional.of(instance);
        }, context);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public MockHandler getHandler(Object mock) {
        return delegate.getHandler(mock);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void resetMock(Object mock, MockHandler newHandler, MockCreationSettings settings) {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            delegate.resetMock(mock, newHandler, settings);
            return null;
        }, context);
    }

    @Override
    @Incubating
    public TypeMockability isTypeMockable(Class<?> type) {
        return delegate.isTypeMockable(type);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public <T> StaticMockControl<T> createStaticMock(Class<T> type, MockCreationSettings<T> settings, MockHandler handler) {
        return delegate.createStaticMock(type, settings, handler);
    }

    @Override
    public <T> ConstructionMockControl<T> createConstructionMock(
        Class<T> type,
        Function<MockedConstruction.Context, MockCreationSettings<T>> settingsFactory,
        Function<MockedConstruction.Context, MockHandler<T>> handlerFactory,
        MockedConstruction.MockInitializer<T> mockInitializer
    ) {
        return delegate.createConstructionMock(type, settingsFactory, handlerFactory, mockInitializer);
    }

    @Override
    public void clearAllCaches() {
        delegate.clearAllCaches();
    }
}
