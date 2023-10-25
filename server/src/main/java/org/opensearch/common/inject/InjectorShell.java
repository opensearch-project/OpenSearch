/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Copyright (C) 2008 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.inject;

import org.opensearch.common.inject.internal.Errors;
import org.opensearch.common.inject.internal.InternalContext;
import org.opensearch.common.inject.internal.InternalFactory;
import org.opensearch.common.inject.internal.ProviderInstanceBindingImpl;
import org.opensearch.common.inject.internal.Scoping;
import org.opensearch.common.inject.internal.SourceProvider;
import org.opensearch.common.inject.internal.Stopwatch;
import org.opensearch.common.inject.spi.Dependency;
import org.opensearch.common.inject.spi.Element;
import org.opensearch.common.inject.spi.Elements;
import org.opensearch.common.inject.spi.InjectionPoint;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;

import static java.util.Collections.emptySet;
import static org.opensearch.common.inject.Scopes.SINGLETON;

/**
 * A partially-initialized injector. See {@link InjectorBuilder}, which uses this to build a tree
 * of injectors in batch.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 *
 * @opensearch.internal
 */
class InjectorShell {

    private final List<Element> elements;
    private final InjectorImpl injector;

    private InjectorShell(List<Element> elements, InjectorImpl injector) {
        this.elements = elements;
        this.injector = injector;
    }

    InjectorImpl getInjector() {
        return injector;
    }

    List<Element> getElements() {
        return elements;
    }

    /**
     * Builder for an injector
     *
     * @opensearch.internal
     */
    static class Builder {
        private final List<Element> elements = new ArrayList<>();
        private final List<Module> modules = new ArrayList<>();

        /**
         * lazily constructed
         */
        private State state;
        private final Stage stage = Stage.DEVELOPMENT;

        void addModules(Iterable<? extends Module> modules) {
            for (Module module : modules) {
                this.modules.add(module);
            }
        }

        /**
         * Synchronize on this before calling {@link #build}.
         */
        Object lock() {
            return getState().lock();
        }

        /**
         * Creates and returns the injector shells for the current modules.
         */
        List<InjectorShell> build(BindingProcessor bindingProcessor, Stopwatch stopwatch, Errors errors) {
            if (state == null) {
                throw new IllegalStateException("no state. Did you remember to lock() ?");
            }

            InjectorImpl injector = new InjectorImpl(state);

            modules.add(0, new RootModule(stage));
            new TypeConverterBindingProcessor(errors).prepareBuiltInConverters(injector);

            elements.addAll(Elements.getElements(stage, modules));
            stopwatch.resetAndLog("Module execution");

            new MessageProcessor(errors).process(injector, elements);
            injector.membersInjectorStore = new MembersInjectorStore(injector);
            stopwatch.resetAndLog("TypeListeners creation");

            new ScopeBindingProcessor(errors).process(injector, elements);
            stopwatch.resetAndLog("Scopes creation");

            new TypeConverterBindingProcessor(errors).process(injector, elements);
            stopwatch.resetAndLog("Converters creation");

            bindInjector(injector);
            bindLogger(injector);
            bindingProcessor.process(injector, elements);
            stopwatch.resetAndLog("Binding creation");

            List<InjectorShell> injectorShells = new ArrayList<>();
            injectorShells.add(new InjectorShell(elements, injector));
            stopwatch.resetAndLog("Private environment creation");

            return injectorShells;
        }

        private State getState() {
            if (state == null) {
                state = new InheritingState();
            }
            return state;
        }
    }

    /**
     * The Injector is a special case because we allow both parent and child injectors to both have
     * a binding for that key.
     */
    private static void bindInjector(InjectorImpl injector) {
        Key<Injector> key = Key.get(Injector.class);
        InjectorFactory injectorFactory = new InjectorFactory(injector);
        injector.state.putBinding(
            key,
            new ProviderInstanceBindingImpl<>(
                injector,
                key,
                SourceProvider.UNKNOWN_SOURCE,
                injectorFactory,
                Scoping.UNSCOPED,
                injectorFactory,
                emptySet()
            )
        );
    }

    /**
     * The factory for the injector
     *
     * @opensearch.internal
     */
    private static class InjectorFactory implements InternalFactory<Injector>, Provider<Injector> {
        private final Injector injector;

        private InjectorFactory(Injector injector) {
            this.injector = injector;
        }

        @Override
        public Injector get(Errors errors, InternalContext context, Dependency<?> dependency) {
            return injector;
        }

        @Override
        public Injector get() {
            return injector;
        }

        @Override
        public String toString() {
            return "Provider<Injector>";
        }
    }

    /**
     * The Logger is a special case because it knows the injection point of the injected member. It's
     * the only binding that does this.
     */
    private static void bindLogger(InjectorImpl injector) {
        Key<Logger> key = Key.get(Logger.class);
        LoggerFactory loggerFactory = new LoggerFactory();
        injector.state.putBinding(
            key,
            new ProviderInstanceBindingImpl<>(
                injector,
                key,
                SourceProvider.UNKNOWN_SOURCE,
                loggerFactory,
                Scoping.UNSCOPED,
                loggerFactory,
                emptySet()
            )
        );
    }

    /**
     * Factory for a logger
     *
     * @opensearch.internal
     */
    private static class LoggerFactory implements InternalFactory<Logger>, Provider<Logger> {
        @Override
        public Logger get(Errors errors, InternalContext context, Dependency<?> dependency) {
            InjectionPoint injectionPoint = dependency.getInjectionPoint();
            return injectionPoint == null
                ? Logger.getAnonymousLogger()
                : Logger.getLogger(injectionPoint.getMember().getDeclaringClass().getName());
        }

        @Override
        public Logger get() {
            return Logger.getAnonymousLogger();
        }

        @Override
        public String toString() {
            return "Provider<Logger>";
        }
    }

    /**
     * The root module
     *
     * @opensearch.internal
     */
    private static class RootModule implements Module {
        final Stage stage;

        private RootModule(Stage stage) {
            this.stage = Objects.requireNonNull(stage, "stage");
        }

        @Override
        public void configure(Binder binder) {
            binder = binder.withSource(SourceProvider.UNKNOWN_SOURCE);
            binder.bind(Stage.class).toInstance(stage);
            binder.bindScope(Singleton.class, SINGLETON);
        }
    }
}
