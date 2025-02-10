/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.plugin.systemindex;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.Bits;

import java.io.IOException;

//copied from org.apache.lucene.index.AllDeletedFilterReader
//https://github.com/apache/lucene-solr/blob/1d85cd783863f75cea133fb9c452302214165a4d/lucene/test-framework/src/java/org/apache/lucene/index/AllDeletedFilterReader.java

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

class EmptyFilterLeafReader extends FilterLeafReader {

    final Bits liveDocs;

    public EmptyFilterLeafReader(LeafReader in) {
        super(in);
        liveDocs = new Bits.MatchNoBits(in.maxDoc());
        assert maxDoc() == 0 || hasDeletions();
    }

    @Override
    public Bits getLiveDocs() {
        return liveDocs;
    }

    @Override
    public int numDocs() {
        return 0;
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return null;
    }

    private static class EmptySubReaderWrapper extends FilterDirectoryReader.SubReaderWrapper {

        @Override
        public LeafReader wrap(final LeafReader reader) {
            return new EmptyFilterLeafReader(reader);
        }

    }

    static class EmptyDirectoryReader extends FilterDirectoryReader {

        public EmptyDirectoryReader(final DirectoryReader in) throws IOException {
            super(in, new EmptySubReaderWrapper());
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(final DirectoryReader in) throws IOException {
            return new EmptyDirectoryReader(in);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }
}
