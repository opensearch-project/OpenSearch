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

package org.opensearch.common.inject.spi;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.inject.Binder;
import org.opensearch.common.inject.Scope;

import java.lang.annotation.Annotation;
import java.util.Objects;

/**
 * Registration of a scope annotation with the scope that implements it. Instances are created
 * explicitly in a module using {@link org.opensearch.common.inject.Binder#bindScope(Class, Scope) bindScope()}
 * statements:
 * <pre>
 *     Scope recordScope = new RecordScope();
 *     bindScope(RecordScoped.class, new RecordScope());</pre>
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * @since 2.0
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class ScopeBinding implements Element {
    private final Object source;
    private final Class<? extends Annotation> annotationType;
    private final Scope scope;

    ScopeBinding(Object source, Class<? extends Annotation> annotationType, Scope scope) {
        this.source = Objects.requireNonNull(source, "source");
        this.annotationType = Objects.requireNonNull(annotationType, "annotationType");
        this.scope = Objects.requireNonNull(scope, "scope");
    }

    @Override
    public Object getSource() {
        return source;
    }

    public Class<? extends Annotation> getAnnotationType() {
        return annotationType;
    }

    public Scope getScope() {
        return scope;
    }

    @Override
    public <T> T acceptVisitor(ElementVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public void applyTo(Binder binder) {
        binder.withSource(getSource()).bindScope(annotationType, scope);
    }
}
