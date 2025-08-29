/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lucene.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.LockFactory;

import java.io.IOException;
import java.nio.file.Path;

public class IOInterceptingDirectory extends FSDirectory {

    private static final Logger logger = LogManager.getLogger(IOInterceptingDirectory.class);
    BufferCache cache;
    FSDirectory delegate;

    public IOInterceptingDirectory(Path path, LockFactory lockFactory,
                                   BufferCache pageCache,
                                   FSDirectory delegate) throws IOException {
        super(path, lockFactory);
        this.cache = pageCache;
        this.delegate = delegate;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        IndexInput underlyingIndexInput = delegate.openInput(name, context);
        return new IOInterceptingIndexInput(underlyingIndexInput,
            name,
            context,
            cache,
            name,
            delegate.getDirectory().resolve(name)
            );

    }

    @Override
    public void close() throws IOException {
        delegate.close();
        super.close();
    }
}
