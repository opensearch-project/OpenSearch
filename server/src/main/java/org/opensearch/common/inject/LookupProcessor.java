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
import org.opensearch.common.inject.spi.ProviderLookup;

/**
 * Handles {@link Binder#getProvider} commands.
 *
 * @author crazybob@google.com (Bob Lee)
 * @author jessewilson@google.com (Jesse Wilson)
 *
 * @opensearch.internal
 */
class LookupProcessor extends AbstractProcessor {

    LookupProcessor(final Errors errors) {
        super(errors);
    }

    @Override
    public <T> Boolean visit(final ProviderLookup<T> lookup) {
        // ensure the provider can be created
        try {
            final Provider<T> provider = injector.getProviderOrThrow(lookup.getKey(), errors);
            lookup.initializeDelegate(provider);
        } catch (ErrorsException e) {
            errors.merge(e.getErrors()); // TODO: source
        }

        return true;
    }
}
