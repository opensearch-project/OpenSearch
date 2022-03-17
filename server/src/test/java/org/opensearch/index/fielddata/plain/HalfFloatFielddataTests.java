/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.fielddata.plain;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.Directory;
import org.opensearch.core.internal.io.IOUtils;
import org.apache.lucene.tests.util.TestUtil;
import org.opensearch.index.fielddata.FieldData;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class HalfFloatFielddataTests extends OpenSearchTestCase {

    public void testSingleValued() throws IOException {
        Directory dir = newDirectory();
        // we need the default codec to check for singletons
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null).setCodec(TestUtil.getDefaultCodec()));
        Document doc = new Document();
        for (IndexableField f : NumberFieldMapper.NumberType.HALF_FLOAT.createFields("half_float", 3f, false, true, false)) {
            doc.add(f);
        }
        w.addDocument(doc);
        final DirectoryReader dirReader = DirectoryReader.open(w);
        LeafReader reader = getOnlyLeafReader(dirReader);
        SortedNumericDoubleValues values = new SortedNumericIndexFieldData.SortedNumericHalfFloatFieldData(reader, "half_float")
            .getDoubleValues();
        assertNotNull(FieldData.unwrapSingleton(values));
        assertTrue(values.advanceExact(0));
        assertEquals(1, values.docValueCount());
        assertEquals(3f, values.nextValue(), 0f);
        IOUtils.close(dirReader, w, dir);
    }

    public void testMultiValued() throws IOException {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null));
        Document doc = new Document();
        for (IndexableField f : NumberFieldMapper.NumberType.HALF_FLOAT.createFields("half_float", 3f, false, true, false)) {
            doc.add(f);
        }
        for (IndexableField f : NumberFieldMapper.NumberType.HALF_FLOAT.createFields("half_float", 2f, false, true, false)) {
            doc.add(f);
        }
        w.addDocument(doc);
        final DirectoryReader dirReader = DirectoryReader.open(w);
        LeafReader reader = getOnlyLeafReader(dirReader);
        SortedNumericDoubleValues values = new SortedNumericIndexFieldData.SortedNumericHalfFloatFieldData(reader, "half_float")
            .getDoubleValues();
        assertNull(FieldData.unwrapSingleton(values));
        assertTrue(values.advanceExact(0));
        assertEquals(2, values.docValueCount());
        assertEquals(2f, values.nextValue(), 0f);
        assertEquals(3f, values.nextValue(), 0f);
        IOUtils.close(dirReader, w, dir);
    }
}
