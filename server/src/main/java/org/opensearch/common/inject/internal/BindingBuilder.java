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

package org.opensearch.common.inject.internal;

import org.opensearch.common.inject.Binder;
import org.opensearch.common.inject.ConfigurationException;
import org.opensearch.common.inject.Key;
import org.opensearch.common.inject.Provider;
import org.opensearch.common.inject.binder.AnnotatedBindingBuilder;
import org.opensearch.common.inject.spi.Element;
import org.opensearch.common.inject.spi.InjectionPoint;
import org.opensearch.common.inject.spi.Message;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;

/**
 * Bind a non-constant key.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 *
 * @opensearch.internal
 */
public class BindingBuilder<T> extends AbstractBindingBuilder<T> implements AnnotatedBindingBuilder<T> {

    public BindingBuilder(Binder binder, List<Element> elements, Object source, Key<T> key) {
        super(binder, elements, source, key);
    }

    @Override
    public BindingBuilder<T> to(Class<? extends T> implementation) {
        Key<? extends T> linkedKey = Key.get(implementation);
        Objects.requireNonNull(linkedKey, "linkedKey");
        checkNotTargetted();
        BindingImpl<T> base = getBinding();
        setBinding(new LinkedBindingImpl<>(base.getSource(), base.getKey(), base.getScoping(), linkedKey));
        return this;
    }

    @Override
    public void toInstance(T instance) {
        checkNotTargetted();

        // lookup the injection points, adding any errors to the binder's errors list
        Set<InjectionPoint> injectionPoints;
        if (instance != null) {
            try {
                injectionPoints = InjectionPoint.forInstanceMethodsAndFields(instance.getClass());
            } catch (ConfigurationException e) {
                for (Message message : e.getErrorMessages()) {
                    binder.addError(message);
                }
                injectionPoints = unmodifiableSet(new HashSet<InjectionPoint>(e.getPartialValue()));
            }
        } else {
            binder.addError(BINDING_TO_NULL);
            injectionPoints = emptySet();
        }

        BindingImpl<T> base = getBinding();
        setBinding(new InstanceBindingImpl<>(base.getSource(), base.getKey(), base.getScoping(), injectionPoints, instance));
    }

    @Override
    public BindingBuilder<T> toProvider(Provider<? extends T> provider) {
        Objects.requireNonNull(provider, "provider");
        checkNotTargetted();

        // lookup the injection points, adding any errors to the binder's errors list
        Set<InjectionPoint> injectionPoints;
        try {
            injectionPoints = InjectionPoint.forInstanceMethodsAndFields(provider.getClass());
        } catch (ConfigurationException e) {
            for (Message message : e.getErrorMessages()) {
                binder.addError(message);
            }
            injectionPoints = unmodifiableSet(new HashSet<InjectionPoint>(e.getPartialValue()));
        }

        BindingImpl<T> base = getBinding();
        setBinding(new ProviderInstanceBindingImpl<>(base.getSource(), base.getKey(), base.getScoping(), injectionPoints, provider));
        return this;
    }

    @Override
    public String toString() {
        return "BindingBuilder<" + getBinding().getKey().getTypeLiteral() + ">";
    }
}
