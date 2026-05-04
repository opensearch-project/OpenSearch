/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.stub;

import org.apache.lucene.index.Term;
import org.opensearch.index.engine.dataformat.DeleteResult;
import org.opensearch.index.engine.dataformat.Deleter;

/**
 * A no-op {@link Deleter} for testing purposes.
 */
public class MockDeleter implements Deleter {

    @Override
    public long generation() {
        return 0;
    }

    @Override
    public DeleteResult deleteDoc(Term uid, Long rowId) {
        return new DeleteResult.Success(1, 1, 1);
    }

    @Override
    public void lock() {}

    @Override
    public boolean tryLock() {
        return true;
    }

    @Override
    public void unlock() {}

    @Override
    public void close() {}
}
