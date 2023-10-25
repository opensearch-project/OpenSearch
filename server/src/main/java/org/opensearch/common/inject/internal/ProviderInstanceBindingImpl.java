/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
Copyright (C) 2007 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.inject.internal;

import org.opensearch.common.inject.Injector;
import org.opensearch.common.inject.Key;
import org.opensearch.common.inject.Provider;
import org.opensearch.common.inject.spi.BindingTargetVisitor;
import org.opensearch.common.inject.spi.InjectionPoint;
import org.opensearch.common.inject.spi.ProviderInstanceBinding;

import java.util.Set;

/**
 * Provider instance binding
 *
 * @opensearch.internal
 */
public final class ProviderInstanceBindingImpl<T> extends BindingImpl<T> implements ProviderInstanceBinding<T> {

    final Provider<? extends T> providerInstance;
    final Set<InjectionPoint> injectionPoints;

    public ProviderInstanceBindingImpl(
        final Injector injector,
        final Key<T> key,
        final Object source,
        final InternalFactory<? extends T> internalFactory,
        final Scoping scoping,
        final Provider<? extends T> providerInstance,
        final Set<InjectionPoint> injectionPoints
    ) {
        super(injector, key, source, internalFactory, scoping);
        this.providerInstance = providerInstance;
        this.injectionPoints = injectionPoints;
    }

    public ProviderInstanceBindingImpl(
        final Object source,
        final Key<T> key,
        final Scoping scoping,
        final Set<InjectionPoint> injectionPoints,
        final Provider<? extends T> providerInstance
    ) {
        super(source, key, scoping);
        this.injectionPoints = injectionPoints;
        this.providerInstance = providerInstance;
    }

    @Override
    public <V> V acceptTargetVisitor(final BindingTargetVisitor<? super T, V> visitor) {
        return visitor.visit(this);
    }

    @Override
    public Provider<? extends T> getProviderInstance() {
        return providerInstance;
    }

    @Override
    public Set<InjectionPoint> getInjectionPoints() {
        return injectionPoints;
    }

    @Override
    public BindingImpl<T> withScoping(final Scoping scoping) {
        return new ProviderInstanceBindingImpl<>(getSource(), getKey(), scoping, injectionPoints, providerInstance);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(ProviderInstanceBinding.class).add("key", getKey())
            .add("source", getSource())
            .add("scope", getScoping())
            .add("provider", providerInstance)
            .toString();
    }
}
