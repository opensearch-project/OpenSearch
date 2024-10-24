/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.util;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A plain iterator
 *
 * @opensearch.internal
 */
public class PlainIterator<T> implements Iterable<T>, Countable {
    private final List<T> elements;

    private AtomicInteger index = new AtomicInteger();

    public PlainIterator(List<T> elements) {
        this.elements = elements;
        reset();
    }

    public void reset() {
        index = new AtomicInteger(0);
    }

    public int remaining() {
        return elements.size() - index.get();
    }

    public T nextOrNull() {
        if (index.get() == elements.size()) {
            return null;
        } else {
            return elements.get(index.getAndIncrement());
        }
    }

    @Override
    public int size() {
        return elements.size();
    }

    public List<T> asList() {
        return Collections.unmodifiableList(elements);
    }

    @Override
    public Iterator<T> iterator() {
        return elements.iterator();
    }
}
