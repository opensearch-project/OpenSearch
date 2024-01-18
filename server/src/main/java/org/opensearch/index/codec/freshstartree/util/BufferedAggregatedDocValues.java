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
package org.opensearch.index.codec.freshstartree.util;

import java.io.IOException;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.packed.PackedLongValues;


/** Buffered aggregated doc values - in memory */
public class BufferedAggregatedDocValues extends NumericDocValues {
    final PackedLongValues.Iterator iter;
    final DocIdSetIterator docsWithField;
    private long value;

    public BufferedAggregatedDocValues(PackedLongValues values, DocIdSetIterator docsWithFields) {
        this.iter = values.iterator();
        this.docsWithField = docsWithFields;
    }

    @Override
    public int docID() {
        return docsWithField.docID();
    }

    @Override
    public int nextDoc()
        throws IOException {
        int docID = docsWithField.nextDoc();
        if (docID != NO_MORE_DOCS) {
            value = iter.next();
        }
        return docID;
    }

    @Override
    public int advance(int target) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean advanceExact(int target)
        throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
        return docsWithField.cost();
    }

    @Override
    public long longValue() {
        return value;
    }
}
