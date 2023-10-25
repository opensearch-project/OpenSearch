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
import org.opensearch.common.inject.internal.ErrorsException;

/**
 * Initializables.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 *
 * @opensearch.internal
 */
class Initializables {

    /**
     * Returns an initializable for an instance that requires no initialization.
     */
    static <T> Initializable<T> of(final T instance) {
        return new Initializable<>() {
            @Override
            public T get(Errors errors) throws ErrorsException {
                return instance;
            }

            @Override
            public String toString() {
                return String.valueOf(instance);
            }
        };
    }
}
