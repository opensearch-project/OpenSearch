/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.search.ReferenceManager;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;

import java.io.IOException;

public class LuceneReaderManager implements EngineReaderManager<OpenSearchDirectoryReader> {
    private final ReferenceManager<OpenSearchDirectoryReader> referenceManager;

    public LuceneReaderManager(ReferenceManager<OpenSearchDirectoryReader> referenceManager) {
        this.referenceManager = referenceManager;
    }


    @Override
    public OpenSearchDirectoryReader acquire() throws IOException {
        return referenceManager.acquire();
    }

    @Override
    public void release(OpenSearchDirectoryReader reader) throws IOException {
        referenceManager.release(reader);
    }

    @Override
    public void addListener(ReferenceManager.RefreshListener listener) {

    }
}
