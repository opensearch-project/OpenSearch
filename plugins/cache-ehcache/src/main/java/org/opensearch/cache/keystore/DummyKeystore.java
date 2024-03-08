/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.keystore;

import org.opensearch.common.cache.keystore.KeyLookupStore;

/**
 * A dummy keystore, which will always report that a key is contained in it.
 */
public class DummyKeystore implements KeyLookupStore<Integer> {
    @Override
    public boolean add(Integer value) {
        return true;
    }

    @Override
    public boolean contains(Integer value) {
        return true;
    }

    @Override
    public boolean remove(Integer value) {
        return true;
    }

    @Override
    public int getSize() {
        return 0;
    }

    @Override
    public long getMemorySizeInBytes() {
        return 0;
    }

    @Override
    public long getMemorySizeCapInBytes() {
        return 0;
    }

    @Override
    public boolean isFull() {
        return false;
    }

    @Override
    public void regenerateStore(Integer[] newValues) {}

    @Override
    public void clear() {}
}
