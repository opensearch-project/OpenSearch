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

import org.opensearch.common.inject.internal.BindingImpl;
import org.opensearch.common.inject.internal.Errors;
import org.opensearch.common.inject.internal.InstanceBindingImpl;
import org.opensearch.common.inject.internal.InternalFactory;
import org.opensearch.common.inject.internal.MatcherAndConverter;
import org.opensearch.common.inject.internal.SourceProvider;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptySet;

/**
 * Inheriting state.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 *
 * @opensearch.internal
 */
class InheritingState implements State {

    // Must be a linked hashmap in order to preserve order of bindings in Modules.
    private final Map<Key<?>, Binding<?>> explicitBindingsMutable = new LinkedHashMap<>();
    private final Map<Key<?>, Binding<?>> explicitBindings = Collections.unmodifiableMap(explicitBindingsMutable);
    private final Map<Class<? extends Annotation>, Scope> scopes = new HashMap<>();
    private final List<MatcherAndConverter> converters = new ArrayList<>();
    private WeakKeySet denylistedKeys = new WeakKeySet();
    private final Object lock;

    InheritingState() {
        this.lock = this;
    }

    @Override
    public State parent() {
        return State.NONE;
    }

    @Override
    @SuppressWarnings("unchecked") // we only put in BindingImpls that match their key types
    public <T> BindingImpl<T> getExplicitBinding(Key<T> key) {
        Binding<?> binding = explicitBindings.get(key);
        return binding != null ? (BindingImpl<T>) binding : State.NONE.getExplicitBinding(key);
    }

    @Override
    public Map<Key<?>, Binding<?>> getExplicitBindingsThisLevel() {
        return explicitBindings;
    }

    @Override
    public void putBinding(Key<?> key, BindingImpl<?> binding) {
        explicitBindingsMutable.put(key, binding);
    }

    @Override
    public Scope getScope(Class<? extends Annotation> annotationType) {
        Scope scope = scopes.get(annotationType);
        return scope != null ? scope : State.NONE.getScope(annotationType);
    }

    @Override
    public void putAnnotation(Class<? extends Annotation> annotationType, Scope scope) {
        scopes.put(annotationType, scope);
    }

    @Override
    public Iterable<MatcherAndConverter> getConvertersThisLevel() {
        return converters;
    }

    @Override
    public void addConverter(MatcherAndConverter matcherAndConverter) {
        converters.add(matcherAndConverter);
    }

    @Override
    public MatcherAndConverter getConverter(String stringValue, TypeLiteral<?> type, Errors errors, Object source) {
        MatcherAndConverter matchingConverter = null;
        for (State s = this; s != State.NONE; s = s.parent()) {
            for (MatcherAndConverter converter : s.getConvertersThisLevel()) {
                if (converter.getTypeMatcher().matches(type)) {
                    if (matchingConverter != null) {
                        errors.ambiguousTypeConversion(stringValue, source, type, matchingConverter, converter);
                    }
                    matchingConverter = converter;
                }
            }
        }
        return matchingConverter;
    }

    @Override
    public void denylist(Key<?> key) {
        State.NONE.denylist(key);
        denylistedKeys.add(key);
    }

    @Override
    public boolean isDenylisted(Key<?> key) {
        return denylistedKeys.contains(key);
    }

    @Override
    public void clearDenylisted() {
        denylistedKeys = new WeakKeySet();
    }

    @Override
    public void makeAllBindingsToEagerSingletons(Injector injector) {
        Map<Key<?>, Binding<?>> x = new LinkedHashMap<>();
        for (Map.Entry<Key<?>, Binding<?>> entry : this.explicitBindingsMutable.entrySet()) {
            Key key = entry.getKey();
            BindingImpl<?> binding = (BindingImpl<?>) entry.getValue();
            Object value = binding.getProvider().get();
            x.put(
                key,
                new InstanceBindingImpl<Object>(
                    injector,
                    key,
                    SourceProvider.UNKNOWN_SOURCE,
                    new InternalFactory.Instance(value),
                    emptySet(),
                    value
                )
            );
        }
        this.explicitBindingsMutable.clear();
        this.explicitBindingsMutable.putAll(x);
    }

    @Override
    public Object lock() {
        return lock;
    }
}
