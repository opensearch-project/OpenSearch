/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils.cache;

import java.util.Deque;

/**
 * An element that is linked on the {@link Deque}.
 *
 * @opensearch.internal
 */
public interface Linked<T extends Linked<T>> {

    /**
     * Retrieves the previous element or null if either the element is
     * unlinked or the first element on the deque.
     */
    T getPrevious();

    /**
     * Sets the previous element or null if there is no link.
     */
    void setPrevious(T prev);

    /**
     * Retrieves the next element or null if either the element is
     * unlinked or the last element on the deque.
     */
    T getNext();

    /**
     * Sets the next element or null if there is no link.
     */
    void setNext(T next);
}
