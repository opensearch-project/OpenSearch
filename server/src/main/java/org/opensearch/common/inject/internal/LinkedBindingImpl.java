/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Copyright (C) 2007 Google Inc.
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

import org.opensearch.common.inject.Injector;
import org.opensearch.common.inject.Key;
import org.opensearch.common.inject.spi.BindingTargetVisitor;
import org.opensearch.common.inject.spi.LinkedKeyBinding;

/**
 * Linked binding implementation
 *
 * @opensearch.internal
 */
public final class LinkedBindingImpl<T> extends BindingImpl<T> implements LinkedKeyBinding<T> {

    final Key<? extends T> targetKey;

    public LinkedBindingImpl(
        Injector injector,
        Key<T> key,
        Object source,
        InternalFactory<? extends T> internalFactory,
        Scoping scoping,
        Key<? extends T> targetKey
    ) {
        super(injector, key, source, internalFactory, scoping);
        this.targetKey = targetKey;
    }

    public LinkedBindingImpl(Object source, Key<T> key, Scoping scoping, Key<? extends T> targetKey) {
        super(source, key, scoping);
        this.targetKey = targetKey;
    }

    @Override
    public <V> V acceptTargetVisitor(BindingTargetVisitor<? super T, V> visitor) {
        return visitor.visit(this);
    }

    @Override
    public Key<? extends T> getLinkedKey() {
        return targetKey;
    }

    @Override
    public BindingImpl<T> withScoping(Scoping scoping) {
        return new LinkedBindingImpl<>(getSource(), getKey(), scoping, targetKey);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(LinkedKeyBinding.class).add("key", getKey())
            .add("source", getSource())
            .add("scope", getScoping())
            .add("target", targetKey)
            .toString();
    }
}
