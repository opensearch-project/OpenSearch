/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Copyright (C) 2006 Google Inc.
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
import org.opensearch.common.inject.internal.ErrorsException;
import org.opensearch.common.inject.internal.InternalContext;
import org.opensearch.common.inject.internal.InternalFactory;
import org.opensearch.common.inject.internal.SourceProvider;
import org.opensearch.common.inject.spi.Dependency;

import java.util.Objects;

/**
 * Adapats internal factory to a provider
 *
 * @author crazybob@google.com (Bob Lee)
 *
 * @opensearch.internal
 */
class InternalFactoryToProviderAdapter<T> implements InternalFactory<T> {

    private final Initializable<Provider<? extends T>> initializable;
    private final Object source;

    InternalFactoryToProviderAdapter(final Initializable<Provider<? extends T>> initializable) {
        this(initializable, SourceProvider.UNKNOWN_SOURCE);
    }

    InternalFactoryToProviderAdapter(final Initializable<Provider<? extends T>> initializable, Object source) {
        this.initializable = Objects.requireNonNull(initializable, "provider");
        this.source = Objects.requireNonNull(source, "source");
    }

    @Override
    public T get(final Errors errors, final InternalContext context, final Dependency<?> dependency) throws ErrorsException {
        try {
            return errors.checkForNull(initializable.get(errors).get(), source, dependency);
        } catch (RuntimeException userException) {
            throw errors.withSource(source).errorInProvider(userException).toException();
        }
    }

    @Override
    public String toString() {
        return initializable.toString();
    }
}
