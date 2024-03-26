/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Copyright (C) 2009 Google Inc.
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
import org.opensearch.common.inject.TypeLiteral;
import org.opensearch.common.inject.matcher.Matcher;

/**
 * Binds types (picked using a Matcher) to an type listener. Registrations are created explicitly in
 * a module using {@link org.opensearch.common.inject.Binder#bindListener(Matcher, TypeListener)} statements:
 * <pre>
 *     register(only(new TypeLiteral&lt;PaymentService&lt;CreditCard&gt;&gt;() {}), listener);</pre>
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * @since 2.0
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class TypeListenerBinding implements Element {

    private final Object source;
    private final Matcher<? super TypeLiteral<?>> typeMatcher;
    private final TypeListener listener;

    TypeListenerBinding(Object source, TypeListener listener, Matcher<? super TypeLiteral<?>> typeMatcher) {
        this.source = source;
        this.listener = listener;
        this.typeMatcher = typeMatcher;
    }

    /**
     * Returns the registered listener.
     */
    public TypeListener getListener() {
        return listener;
    }

    /**
     * Returns the type matcher which chooses which types the listener should be notified of.
     */
    public Matcher<? super TypeLiteral<?>> getTypeMatcher() {
        return typeMatcher;
    }

    @Override
    public Object getSource() {
        return source;
    }

    @Override
    public <T> T acceptVisitor(ElementVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public void applyTo(Binder binder) {
        binder.withSource(getSource()).bindListener(typeMatcher, listener);
    }
}
