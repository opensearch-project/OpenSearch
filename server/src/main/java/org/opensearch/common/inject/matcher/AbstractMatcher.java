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

package org.opensearch.common.inject.matcher;

/**
 * Implements {@code and()} and {@code or()}.
 *
 * @author crazybob@google.com (Bob Lee)
 *
 * @opensearch.internal
 */
public abstract class AbstractMatcher<T> implements Matcher<T> {

    @Override
    public Matcher<T> and(final Matcher<? super T> other) {
        return new AndMatcher<>(this, other);
    }

    @Override
    public Matcher<T> or(Matcher<? super T> other) {
        return new OrMatcher<>(this, other);
    }

    /**
     * An AND matcher
     *
     * @opensearch.internal
     */
    private static class AndMatcher<T> extends AbstractMatcher<T> {
        private final Matcher<? super T> a, b;

        AndMatcher(Matcher<? super T> a, Matcher<? super T> b) {
            this.a = a;
            this.b = b;
        }

        @Override
        public boolean matches(T t) {
            return a.matches(t) && b.matches(t);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof AndMatcher && ((AndMatcher<?>) other).a.equals(a) && ((AndMatcher<?>) other).b.equals(b);
        }

        @Override
        public int hashCode() {
            return 41 * (a.hashCode() ^ b.hashCode());
        }

        @Override
        public String toString() {
            return "and(" + a + ", " + b + ")";
        }
    }

    /**
     * An OR matcher
     *
     * @opensearch.internal
     */
    private static class OrMatcher<T> extends AbstractMatcher<T> {
        private final Matcher<? super T> a, b;

        OrMatcher(Matcher<? super T> a, Matcher<? super T> b) {
            this.a = a;
            this.b = b;
        }

        @Override
        public boolean matches(T t) {
            return a.matches(t) || b.matches(t);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof OrMatcher && ((OrMatcher<?>) other).a.equals(a) && ((OrMatcher<?>) other).b.equals(b);
        }

        @Override
        public int hashCode() {
            return 37 * (a.hashCode() ^ b.hashCode());
        }

        @Override
        public String toString() {
            return "or(" + a + ", " + b + ")";
        }
    }
}
