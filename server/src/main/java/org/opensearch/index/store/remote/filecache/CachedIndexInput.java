/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.apache.lucene.store.IndexInput;

/**
 * Base IndexInput whose instances will be maintained in cache.
 *
 * @opensearch.internal
 */
public abstract class CachedIndexInput extends IndexInput {

    /**
     * resourceDescription should be a non-null, opaque string
     * describing this resource; it's returned from
     * {@link #toString}.
     */
    protected CachedIndexInput(String resourceDescription) {
        super(resourceDescription);
    }

    /**
     * return true this index input is closed, false if not
     * @return true this index input is closed, false if not
     */
    public abstract boolean isClosed();
}
