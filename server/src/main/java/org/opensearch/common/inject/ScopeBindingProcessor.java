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

import org.opensearch.common.inject.internal.Annotations;
import org.opensearch.common.inject.internal.Errors;
import org.opensearch.common.inject.spi.ScopeBinding;

import java.lang.annotation.Annotation;
import java.util.Objects;

/**
 * Handles {@link Binder#bindScope} commands.
 *
 * @author crazybob@google.com (Bob Lee)
 * @author jessewilson@google.com (Jesse Wilson)
 *
 * @opensearch.internal
 */
class ScopeBindingProcessor extends AbstractProcessor {

    ScopeBindingProcessor(final Errors errors) {
        super(errors);
    }

    @Override
    public Boolean visit(final ScopeBinding command) {
        final Scope scope = command.getScope();
        final Class<? extends Annotation> annotationType = command.getAnnotationType();

        if (!Annotations.isScopeAnnotation(annotationType)) {
            errors.withSource(annotationType).missingScopeAnnotation();
            // Go ahead and bind anyway so we don't get collateral errors.
        }

        if (!Annotations.isRetainedAtRuntime(annotationType)) {
            errors.withSource(annotationType).missingRuntimeRetention(command.getSource());
            // Go ahead and bind anyway so we don't get collateral errors.
        }

        final Scope existing = injector.state.getScope(Objects.requireNonNull(annotationType, "annotation type"));
        if (existing != null) {
            errors.duplicateScopes(existing, annotationType, scope);
        } else {
            injector.state.putAnnotation(annotationType, Objects.requireNonNull(scope, "scope"));
        }

        return true;
    }
}
