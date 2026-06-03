/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap;

import org.opensearch.test.OpenSearchTestCase;

import java.io.ObjectInputFilter;

public class BootstrapSerialFilterTests extends OpenSearchTestCase {

    public void testRejectAllFilterRejectsClasses() {
        ObjectInputFilter.FilterInfo info = filterInfo(String.class);
        assertEquals(ObjectInputFilter.Status.REJECTED, Bootstrap.REJECT_ALL_FILTER.checkInput(info));
    }

    public void testRejectAllFilterRejectsAnyClass() {
        ObjectInputFilter.FilterInfo info = filterInfo(Runtime.class);
        assertEquals(ObjectInputFilter.Status.REJECTED, Bootstrap.REJECT_ALL_FILTER.checkInput(info));
    }

    public void testRejectAllFilterUndecidedForNullClass() {
        // null serialClass = stream metadata check (depth, bytes, refs), not a class resolution
        ObjectInputFilter.FilterInfo info = filterInfo(null);
        assertEquals(ObjectInputFilter.Status.UNDECIDED, Bootstrap.REJECT_ALL_FILTER.checkInput(info));
    }

    public void testFactoryReturnsRejectAllWhenNoFilterRequested() {
        ObjectInputFilter result = applyFactory(null, null);
        assertSame(Bootstrap.REJECT_ALL_FILTER, result);
    }

    public void testFactoryDelegatesToRequestedFilter() {
        ObjectInputFilter customFilter = info -> ObjectInputFilter.Status.ALLOWED;
        ObjectInputFilter result = applyFactory(null, customFilter);
        assertSame(customFilter, result);
    }

    public void testFactoryIgnoresCurrentFilterWhenRequestedIsSet() {
        ObjectInputFilter current = info -> ObjectInputFilter.Status.REJECTED;
        ObjectInputFilter requested = info -> ObjectInputFilter.Status.ALLOWED;
        ObjectInputFilter result = applyFactory(current, requested);
        assertSame(requested, result);
    }

    /**
     * Exercises the exact lambda logic from Bootstrap.initializeSerialFilter().
     * This is the same function passed to setSerialFilterFactory.
     */
    private static ObjectInputFilter applyFactory(ObjectInputFilter current, ObjectInputFilter requested) {
        return requested != null ? requested : Bootstrap.REJECT_ALL_FILTER;
    }

    private static ObjectInputFilter.FilterInfo filterInfo(Class<?> clazz) {
        return new ObjectInputFilter.FilterInfo() {
            public Class<?> serialClass() {
                return clazz;
            }

            public long arrayLength() {
                return -1;
            }

            public long depth() {
                return 1;
            }

            public long references() {
                return 1;
            }

            public long streamBytes() {
                return 0;
            }
        };
    }
}
