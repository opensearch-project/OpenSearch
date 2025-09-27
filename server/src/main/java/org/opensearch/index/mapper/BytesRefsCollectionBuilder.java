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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Purposed for passing terms into {@link TermInSetQuery}.
 * If the given terms are sorted already, it wraps terms with a SortedSet stub.
 * Otherwise, it passes terms as a list.
 */
public class BytesRefsCollectionBuilder implements Consumer<BytesRef>, Supplier<Collection<BytesRef>> {

    /**
     * Strategy for building BytesRef collection.
     * */
    protected interface ConsumerStrategy extends Function<BytesRef, ConsumerStrategy>, Supplier<Collection<BytesRef>> {}

    public BytesRefsCollectionBuilder(int sizeExpected) {
        terms = new ArrayList<>(sizeExpected);
    }

    protected final List<BytesRef> terms;
    protected ConsumerStrategy delegate = createStartStrategy();

    @Override
    public void accept(BytesRef bytesRef) {
        delegate = delegate.apply(bytesRef);
    }

    @Override
    public Collection<BytesRef> get() {
        Collection<BytesRef> result = delegate.get();
        delegate = createFrozenStrategy(result);
        return result;
    }

    protected ConsumerStrategy createStartStrategy() {
        return new ConsumerStrategy() {
            @Override
            public ConsumerStrategy apply(BytesRef firstBytes) {
                terms.add(firstBytes); // firstly, just store
                return createSortedStrategy(firstBytes);
            }

            @Override
            public Collection<BytesRef> get() {
                return terms; // empty list
            }
        };
    }

    protected ConsumerStrategy createSortedStrategy(BytesRef firstBytes) {
        return new ConsumerStrategy() {
            BytesRef prev = firstBytes;

            @Override
            public ConsumerStrategy apply(BytesRef bytesRef) {
                terms.add(bytesRef);
                if (bytesRef.compareTo(prev) >= 0) { // keep checking sorted
                    prev = bytesRef;
                    return this;
                } else { // isn't sorted
                    return createUnsortedStrategy();
                }
            }

            @Override
            public Collection<BytesRef> get() {
                return new SortedBytesSet(terms);
            }
        };
    }

    protected ConsumerStrategy createUnsortedStrategy() {
        return new ConsumerStrategy() {
            @Override
            public ConsumerStrategy apply(BytesRef bytesRef) { // just storing
                terms.add(bytesRef);
                return this;
            }

            @Override
            public Collection<BytesRef> get() {
                return terms;
            }
        };
    }

    protected ConsumerStrategy createFrozenStrategy(Collection<BytesRef> result) {
        return new ConsumerStrategy() {

            @Override
            public ConsumerStrategy apply(BytesRef bytesRef) {
                throw new IllegalStateException("already build");
            }

            @Override
            public Collection<BytesRef> get() {
                return result;
            }
        };
    }

    /**
     * {@link SortedSet<BytesRef>} for passing into TermInSetQuery()
     * */
    protected static class SortedBytesSet extends AbstractSet<BytesRef> implements SortedSet<BytesRef> {

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

        @Override
        public SortedSet<BytesRef> subSet(BytesRef fromElement, BytesRef toElement) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SortedSet<BytesRef> headSet(BytesRef toElement) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SortedSet<BytesRef> tailSet(BytesRef fromElement) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytesRef first() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytesRef last() {
            throw new UnsupportedOperationException();
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
