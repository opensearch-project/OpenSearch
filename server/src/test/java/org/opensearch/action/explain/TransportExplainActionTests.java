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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.explain;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.lucene.uid.VersionsAndSeqNoResolver;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.get.DocumentLookupResult;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link TransportExplainAction#resolveTopLevelDocId}, which resolves the top-level doc id used to
 * explain a hit against the explain searcher's own reader.
 */
public class TransportExplainActionTests extends OpenSearchTestCase {

    private static Engine.GetResult preMaterialized(String id) {
        DocumentLookupResult lookup = new DocumentLookupResult(id, 1L, true, null, 0L, 1L, Map.of(), Map.of());
        return lookup.toGetResult();
    }

    private static StringField idField(String id) {
        return new StringField(IdFieldMapper.NAME, Uid.encodeId(id), Field.Store.YES);
    }

    private static Term idTerm(String id) {
        return new Term(IdFieldMapper.NAME, Uid.encodeId(id));
    }

    // PreMaterialized + single segment: id present, helper returns the leaf docId with docBase 0.
    public void testPreMaterializedResolvesDocIdInSingleSegment() throws Exception {
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
                writer.addDocument(List.of(idField("1")));
                writer.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                int docId = TransportExplainAction.resolveTopLevelDocId(preMaterialized("1"), reader, idTerm("1"));
                assertEquals(0, docId);
            }
        }
    }

    // PreMaterialized across segments: target lands in a later segment, helper lifts the leaf docId by its docBase.
    public void testPreMaterializedAppliesDocBaseAcrossSegments() throws Exception {
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
                for (int i = 0; i < 5; i++) {
                    writer.addDocument(List.of(idField("first-" + i)));
                }
                writer.commit();
                writer.addDocument(List.of(idField("target")));
                writer.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                Term term = idTerm("target");
                int docId = TransportExplainAction.resolveTopLevelDocId(preMaterialized("target"), reader, term);

                assertTrue("target must land in a later segment to exercise the docBase lift", docId >= reader.leaves().get(1).docBase);

                // Strongest proof the docBase lift is correct: the stored _id at the top-level docId is "target".
                BytesRef stored = reader.storedFields().document(docId).getBinaryValue(IdFieldMapper.NAME);
                assertEquals(Uid.encodeId("target"), stored);
            }
        }
    }

    // PreMaterialized + id term absent from the reader: helper returns NO_MATCH rather than erroring.
    public void testPreMaterializedMissingIdReturnsNoMatch() throws Exception {
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
                writer.addDocument(List.of(idField("present")));
                writer.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                int docId = TransportExplainAction.resolveTopLevelDocId(preMaterialized("absent"), reader, idTerm("absent"));
                assertEquals(TransportExplainAction.NO_MATCH, docId);
            }
        }
    }

    // Non-PreMaterialized: helper reads docIdAndVersion (docId + docBase) and never touches the reader.
    public void testNonPreMaterializedUsesDocIdAndVersion() throws Exception {
        VersionsAndSeqNoResolver.DocIdAndVersion docIdAndVersion = new VersionsAndSeqNoResolver.DocIdAndVersion(3, 1L, 0L, 1L, null, 10);
        Engine.GetResult result = new Engine.GetResult(null, docIdAndVersion, false);
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
                writer.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                int docId = TransportExplainAction.resolveTopLevelDocId(result, reader, idTerm("ignored"));
                assertEquals(13, docId);
            }
        }
    }
}
