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
import org.opensearch.common.inject.internal.InternalFactory;
import org.opensearch.common.inject.spi.Dependency;

/**
 * Adapts a provider to an internal factory.
 *
 * @author crazybob@google.com (Bob Lee)
 *
 * @opensearch.internal
 */
class ProviderToInternalFactoryAdapter<T> implements Provider<T> {

    private final InjectorImpl injector;
    private final InternalFactory<? extends T> internalFactory;

    ProviderToInternalFactoryAdapter(InjectorImpl injector, InternalFactory<? extends T> internalFactory) {
        this.injector = injector;
        this.internalFactory = internalFactory;
    }

    @Override
    public T get() {
        final Errors errors = new Errors();
        try {
            T t = injector.callInContext((ContextualCallable<T>) context -> {
                Dependency dependency = context.getDependency();
                return internalFactory.get(errors, context, dependency);
            });
            errors.throwIfNewErrors(0);
            return t;
        } catch (ErrorsException e) {
            throw new ProvisionException(errors.merge(e.getErrors()).getMessages());
        }
    }

    @Override
    public String toString() {
        return internalFactory.toString();
    }
}
