/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BytesRef;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Purposed for passing terms into {@link TermInSetQuery}
 */
public class BytesRefsCollectionBuilder implements Consumer<BytesRef>, Supplier<Collection<BytesRef>> {

    private interface BytesCollector extends Function<BytesRef, BytesCollector>, Supplier<Collection<BytesRef>> {}

    private final List<BytesRef> terms = new ArrayList<>();
    private BytesCollector delegate = new Start();

    @Override
    public void accept(BytesRef bytesRef) {
        delegate = delegate.apply(bytesRef);
    }

    @Override
    public Collection<BytesRef> get() {
        Collection<BytesRef> result = delegate.get();
        delegate = new Frozen(result);
        return result;
    }

    private class Start implements BytesCollector {
        @Override
        public BytesCollector apply(BytesRef firstBytes) {
            terms.add(firstBytes); // firstly, just store
            return new Sorted(firstBytes);
        }

        @Override
        public Collection<BytesRef> get() {
            return terms; // empty list
        }
    }

    private class Sorted implements BytesCollector {
        BytesRef prev;

        public Sorted(BytesRef firstBytes) {
            prev = firstBytes;
        }

        @Override
        public BytesCollector apply(BytesRef bytesRef) {
            terms.add(bytesRef);
            if (bytesRef.compareTo(prev) >= 0) { // keep checking sorted
                prev = bytesRef;
                return this;
            } else { // isn't sorted
                return new NotSorted();
            }
        }

        @Override
        public Collection<BytesRef> get() {
            return new SortedBytesSet(terms);
        }
    }

    private class NotSorted implements BytesCollector {
        @Override
        public BytesCollector apply(BytesRef bytesRef) { // just storing
            terms.add(bytesRef);
            return this;
        }

        @Override
        public Collection<BytesRef> get() {
            return terms; // empty list
        }
    }

    private static class Frozen implements BytesCollector {
        private final Collection<BytesRef> result;

        public Frozen(Collection<BytesRef> result) {
            this.result = result;
        }

        @Override
        public BytesCollector apply(BytesRef bytesRef) {
            throw new IllegalStateException("already build");
        }

        @Override
        public Collection<BytesRef> get() {
            return result;
        }
    }

    static class SortedBytesSet extends AbstractSet<BytesRef> implements SortedSet<BytesRef> {

        private final List<BytesRef> bytesRefs;

        public SortedBytesSet(List<BytesRef> bytesRefs) {
            this.bytesRefs = bytesRefs;
        }

        @Override
        public Iterator<BytesRef> iterator() {
            return bytesRefs.iterator();
        }

        @Override
        public int size() {
            return bytesRefs.size();
        }

        @Override
        public Comparator<? super BytesRef> comparator() {
            return null;
        }

        /**
         * NSFW
         */
        @Override
        public SortedSet<BytesRef> subSet(BytesRef fromElement, BytesRef toElement) {
            int fromIdx = Collections.binarySearch(bytesRefs, fromElement);
            int toIdx = Collections.binarySearch(bytesRefs, toElement);
            return new SortedBytesSet(bytesRefs.subList(fromIdx >= 0 ? fromIdx : -fromIdx + 1, toIdx >= 0 ? toIdx : -toIdx + 1));
        }

        /**
         * NSFW
         */
        @Override
        public SortedSet<BytesRef> headSet(BytesRef toElement) {
            int toIdx = Collections.binarySearch(bytesRefs, toElement);
            return new SortedBytesSet(bytesRefs.subList(0, toIdx >= 0 ? toIdx : -toIdx + 1));
        }

        /**
         * NSFW
         */
        @Override
        public SortedSet<BytesRef> tailSet(BytesRef fromElement) {
            int fromIdx = Collections.binarySearch(bytesRefs, fromElement);
            return new SortedBytesSet(bytesRefs.subList(fromIdx >= 0 ? fromIdx : -fromIdx + 1, bytesRefs.size()));
        }

        @Override
        public BytesRef first() {
            return bytesRefs.getFirst();
        }

        @Override
        public BytesRef last() {
            return bytesRefs.getLast();
        }

        /**
         * Dedicated for {@link TermInSetQuery#TermInSetQuery(String, Collection)}.
         */
        @Override
        public <T> T[] toArray(T[] a) {
            return bytesRefs.toArray(a);
        }
    }
}
