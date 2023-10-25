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

import org.opensearch.common.inject.spi.InjectionPoint;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Produces construction proxies that invoke the class constructor.
 *
 * @author crazybob@google.com (Bob Lee)
 *
 * @opensearch.internal
 */
class DefaultConstructionProxyFactory<T> implements ConstructionProxyFactory<T> {

    private final InjectionPoint injectionPoint;

    /**
     * @param injectionPoint an injection point whose member is a constructor of {@code T}.
     */
    DefaultConstructionProxyFactory(InjectionPoint injectionPoint) {
        this.injectionPoint = injectionPoint;
    }

    @Override
    public ConstructionProxy<T> create() {
        @SuppressWarnings("unchecked") // the injection point is for a constructor of T
        final Constructor<T> constructor = (Constructor<T>) injectionPoint.getMember();

        return new ConstructionProxy<>() {
            @Override
            public T newInstance(Object... arguments) throws InvocationTargetException {
                try {
                    return constructor.newInstance(arguments);
                } catch (InstantiationException e) {
                    throw new AssertionError(e); // shouldn't happen, we know this is a concrete type
                } catch (IllegalAccessException e) {
                    // a security manager is blocking us, we're hosed
                    throw new AssertionError("Wrong access modifiers on " + constructor, e);
                }
            }

            @Override
            public InjectionPoint getInjectionPoint() {
                return injectionPoint;
            }
        };
    }
}
