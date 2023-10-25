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

package org.opensearch.common.inject.multibindings;

import org.opensearch.common.inject.Binder;
import org.opensearch.common.inject.Binding;
import org.opensearch.common.inject.ConfigurationException;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.inject.Key;
import org.opensearch.common.inject.Module;
import org.opensearch.common.inject.Provider;
import org.opensearch.common.inject.TypeLiteral;
import org.opensearch.common.inject.binder.LinkedBindingBuilder;
import org.opensearch.common.inject.internal.Errors;
import org.opensearch.common.inject.spi.Message;
import org.opensearch.common.inject.util.Types;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;

/**
 * An API to bind multiple values separately, only to later inject them as a
 * complete collection. Multibinder is intended for use in your application's
 * module:
 * <pre><code>
 * public class SnacksModule extends AbstractModule {
 *   protected void configure() {
 *     Multibinder&lt;Snack&gt; multibinder
 *         = Multibinder.newSetBinder(binder(), Snack.class);
 *     multibinder.addBinding().toInstance(new Twix());
 *     multibinder.addBinding().toProvider(SnickersProvider.class);
 *     multibinder.addBinding().to(Skittles.class);
 *   }
 * }</code></pre>
 * <p>
 * With this binding, a {@link Set}{@code <Snack>} can now be injected:
 * <pre><code>
 * class SnackMachine {
 *   {@literal @}Inject
 *   public SnackMachine(Set&lt;Snack&gt; snacks) { ... }
 * }</code></pre>
 * <p>
 * Create multibindings from different modules is supported. For example, it
 * is okay to have both {@code CandyModule} and {@code ChipsModule} to both
 * create their own {@code Multibinder<Snack>}, and to each contribute bindings
 * to the set of snacks. When that set is injected, it will contain elements
 * from both modules.
 * <p>
 * Elements are resolved at set injection time. If an element is bound to a
 * provider, that provider's get method will be called each time the set is
 * injected (unless the binding is also scoped).
 * <p>
 * Annotations are be used to create different sets of the same element
 * type. Each distinct annotation gets its own independent collection of
 * elements.
 * <p>
 * <strong>Elements must be distinct.</strong> If multiple bound elements
 * have the same value, set injection will fail.
 * <p>
 * <strong>Elements must be non-null.</strong> If any set element is null,
 * set injection will fail.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 *
 * @opensearch.internal
 */
public abstract class Multibinder<T> {
    private Multibinder() {}

    /**
     * Returns a new multibinder that collects instances of {@code type} in a {@link Set} that is
     * itself bound with no binding annotation.
     */
    public static <T> Multibinder<T> newSetBinder(Binder binder, TypeLiteral<T> type) {
        binder = binder.skipSources(RealMultibinder.class, Multibinder.class);
        RealMultibinder<T> result = new RealMultibinder<>(binder, type, Key.get(Multibinder.setOf(type)));
        binder.install(result);
        return result;
    }

    @SuppressWarnings("unchecked") // wrapping a T in a Set safely returns a Set<T>
    private static <T> TypeLiteral<Set<T>> setOf(TypeLiteral<T> elementType) {
        Type type = Types.setOf(elementType.getType());
        return (TypeLiteral<Set<T>>) TypeLiteral.get(type);
    }

    /**
     * The actual multibinder plays several roles:
     * <p>
     * As a Multibinder, it acts as a factory for LinkedBindingBuilders for
     * each of the set's elements. Each binding is given an annotation that
     * identifies it as a part of this set.
     * <p>
     * As a Module, it installs the binding to the set itself. As a module,
     * this implements equals() and hashcode() in order to trick Guice into
     * executing its configure() method only once. That makes it so that
     * multiple multibinders can be created for the same target collection, but
     * only one is bound. Since the list of bindings is retrieved from the
     * injector itself (and not the multibinder), each multibinder has access to
     * all contributions from all multibinders.
     * <p>
     * As a Provider, this constructs the set instances.
     * <p>
     * We use a subclass to hide 'implements Module, Provider' from the public
     * API.
     */
    public static final class RealMultibinder<T> extends Multibinder<T> implements Module, Provider<Set<T>> {

        private final TypeLiteral<T> elementType;
        private final Key<Set<T>> setKey;

        /* the target injector's binder. non-null until initialization, null afterwards */
        private Binder binder;

        /* a provider for each element in the set. null until initialization, non-null afterwards */
        private List<Provider<T>> providers;

        private RealMultibinder(final Binder binder, final TypeLiteral<T> elementType, final Key<Set<T>> setKey) {
            this.binder = Objects.requireNonNull(binder, "binder");
            this.elementType = Objects.requireNonNull(elementType, "elementType");
            this.setKey = Objects.requireNonNull(setKey, "setKey");
        }

        @Override
        public void configure(final Binder binder) {
            checkConfiguration(!isInitialized(), "Multibinder was already initialized");

            binder.bind(setKey).toProvider(this);
        }

        public LinkedBindingBuilder<T> addBinding() {
            checkConfiguration(!isInitialized(), "Multibinder was already initialized");

            return binder.bind(Key.get(elementType, new RealElement("")));
        }

        /**
         * Invoked by Guice at Injector-creation time to prepare providers for each
         * element in this set. At this time the set's size is known, but its
         * contents are only evaluated when get() is invoked.
         */
        @Inject
        public void initialize(Injector injector) {
            providers = new ArrayList<>();
            for (Binding<?> entry : injector.findBindingsByType(elementType)) {
                final Key<?> key = entry.getKey();
                if (key.getTypeLiteral().equals(elementType)
                    && key.getAnnotation() instanceof Element
                    && ((Element) key.getAnnotation()).setName().equals("")) {
                    @SuppressWarnings("unchecked") // protected by findBindingsByType()
                    Binding<T> binding = (Binding<T>) entry;
                    providers.add(binding.getProvider());
                }
            }
            this.binder = null;
        }

        private boolean isInitialized() {
            return binder == null;
        }

        @Override
        public Set<T> get() {
            checkConfiguration(isInitialized(), "Multibinder is not initialized");

            Set<T> result = new LinkedHashSet<>();
            for (Provider<T> provider : providers) {
                final T newValue = provider.get();
                checkConfiguration(newValue != null, "Set injection failed due to null element");
                checkConfiguration(result.add(newValue), "Set injection failed due to duplicated element \"%s\"", newValue);
            }
            return Collections.unmodifiableSet(result);
        }

        String getSetName() {
            return "";
        }

        Key<Set<T>> getSetKey() {
            return setKey;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof RealMultibinder && ((RealMultibinder<?>) o).setKey.equals(setKey);
        }

        @Override
        public int hashCode() {
            return setKey.hashCode();
        }

        @Override
        public String toString() {
            return new StringBuilder().append("Multibinder<").append(elementType).append(">").toString();
        }
    }

    static void checkConfiguration(boolean condition, String format, Object... args) {
        if (condition) {
            return;
        }

        throw new ConfigurationException(singleton(new Message(Errors.format(format, args))));
    }

    static <T> T checkNotNull(T reference, String name) {
        if (reference != null) {
            return reference;
        }

        NullPointerException npe = new NullPointerException(name);
        throw new ConfigurationException(singleton(new Message(emptyList(), npe)));
    }
}
