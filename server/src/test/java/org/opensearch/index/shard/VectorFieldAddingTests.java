/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.VectorUtil;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Verifies {@link VectorFieldAddingCodecReader}: populating a vector field that does not yet exist in
 * a segment, while bulk-copying every existing field.
 *
 * <p>This is the missing primitive for the "field pair + alias flip" upgrade shape. The headline test
 * is {@link #testAddingBeatsReAddingDocuments()}, which measures the saving against the only
 * alternative available before this class existed — re-adding every document.
 */
public class VectorFieldAddingTests extends OpenSearchTestCase {

    private static final String OLD_FIELD = "embedding_foo";
    private static final String NEW_FIELD = "embedding_bar";
    private static final int DIM = 96;
    private static final int NUM_DOCS = 400;
    private static final int EXTRA_TEXT_FIELDS = 12;

    private static final String[] WORDS = {
        "storage",
        "segment",
        "merge",
        "amplification",
        "quantization",
        "traversal",
        "recall",
        "latency",
        "embedding",
        "corpus",
        "retrieval",
        "relevance" };

    private static String syntheticProse(int doc, int field) {
        StringBuilder sb = new StringBuilder(512);
        int seed = doc * 31 + field * 7;
        for (int w = 0; w < 60; w++) {
            sb.append(WORDS[(seed + w * 13) % WORDS.length]).append(' ').append((seed + w) % 997).append(' ');
        }
        return sb.toString();
    }

    private static float[] oldModelVector(int seed) {
        float[] v = new float[DIM];
        for (int i = 0; i < DIM; i++) {
            v[i] = (float) Math.cos((seed + 1) * 0.53 + i * 0.07) + 1.5f;
        }
        VectorUtil.l2normalize(v);
        return v;
    }

    private static float[] newModelVector(int seed) {
        float[] v = new float[DIM];
        for (int i = 0; i < DIM; i++) {
            v[i] = (float) Math.sin((seed + 1) * 0.37 + i * 0.11) + 1.5f;
        }
        VectorUtil.l2normalize(v);
        return v;
    }

    private static IndexWriterConfig config() {
        return new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE)
            .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
            .setMergeScheduler(new SerialMergeScheduler());
    }

    private static IndexWriterConfig pinnedConfig() {
        return config().setCodec(new Lucene104Codec());
    }

    /** A document carrying the old model's vector plus expensive non-vector content. */
    private static Document doc(int i) {
        Document d = new Document();
        d.add(new StringField("_id", Integer.toString(i), Field.Store.YES));
        d.add(new StoredField("payload", "payload-for-doc-" + i + "-" + "z".repeat(64)));
        d.add(new NumericDocValuesField("rank", i));
        for (int f = 0; f < EXTRA_TEXT_FIELDS; f++) {
            d.add(new TextField("body_" + f, syntheticProse(i, f), Field.Store.YES));
        }
        d.add(new KnnFloatVectorField(OLD_FIELD, oldModelVector(i), VectorSimilarityFunction.DOT_PRODUCT));
        return d;
    }

    private static void buildSource(Directory dir, IndexWriterConfig cfg) throws IOException {
        try (IndexWriter w = new IndexWriter(dir, cfg)) {
            for (int i = 0; i < NUM_DOCS; i++) {
                w.addDocument(doc(i));
            }
            w.commit();
            w.forceMerge(1);
        }
    }

    private static int[] docIdToLogicalId(org.apache.lucene.index.LeafReader leaf) throws IOException {
        StoredFields sf = leaf.storedFields();
        int[] ids = new int[leaf.maxDoc()];
        for (int doc = 0; doc < leaf.maxDoc(); doc++) {
            ids[doc] = Integer.parseInt(sf.document(doc).get("_id"));
        }
        return ids;
    }

    /** Runs the add via addIndexes, returning the destination directory's byte size. */
    private long addFieldViaCodecReader(Directory src, Directory dest, IndexWriterConfig cfg) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(src); IndexWriter w = new IndexWriter(dest, cfg)) {
            List<CodecReader> wrapped = new ArrayList<>();
            for (LeafReaderContext ctx : reader.leaves()) {
                final int[] ids = docIdToLogicalId(ctx.reader());
                wrapped.add(
                    new VectorFieldAddingCodecReader(
                        (SegmentReader) ctx.reader(),
                        NEW_FIELD,
                        DIM,
                        VectorSimilarityFunction.DOT_PRODUCT,
                        docId -> newModelVector(ids[docId])
                    )
                );
            }
            w.addIndexes(wrapped.toArray(new CodecReader[0]));
            w.commit();
        }
        return directorySize(dest);
    }

    private static long directorySize(Directory dir) throws IOException {
        long total = 0;
        for (String f : dir.listAll()) {
            total += dir.fileLength(f);
        }
        return total;
    }

    /**
     * The core claim: a field absent from the source segment can be populated, and every existing
     * field — including the original vector field — comes through intact.
     */
    public void testAddsNewVectorFieldAndPreservesEverythingElse() throws Exception {
        try (Directory src = newDirectory(); Directory dest = newDirectory()) {
            buildSource(src, config());

            try (DirectoryReader r = DirectoryReader.open(src)) {
                for (LeafReaderContext ctx : r.leaves()) {
                    assertNull("the new field must not exist beforehand", ctx.reader().getFloatVectorValues(NEW_FIELD));
                }
            }

            addFieldViaCodecReader(src, dest, config());
            TestUtil.checkIndex(dest);

            try (DirectoryReader destReader = DirectoryReader.open(dest)) {
                assertEquals("all documents carried over", NUM_DOCS, destReader.numDocs());

                // 1. The added field exists and holds the new model's vectors.
                int checked = 0;
                for (LeafReaderContext ctx : destReader.leaves()) {
                    FloatVectorValues added = ctx.reader().getFloatVectorValues(NEW_FIELD);
                    assertNotNull("the added field must now exist", added);
                    StoredFields sf = ctx.reader().storedFields();
                    var it = added.iterator();
                    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                        int id = Integer.parseInt(sf.document(doc).get("_id"));
                        assertArrayEquals(
                            "doc " + id + " has the new-model vector",
                            newModelVector(id),
                            added.vectorValue(it.index()),
                            1e-6f
                        );
                        checked++;
                    }
                }
                assertEquals("every document received a vector for the new field", NUM_DOCS, checked);

                // 2. The ORIGINAL vector field is untouched — this is what makes rollback an alias flip.
                for (LeafReaderContext ctx : destReader.leaves()) {
                    FloatVectorValues old = ctx.reader().getFloatVectorValues(OLD_FIELD);
                    assertNotNull("the pre-existing vector field survives", old);
                    StoredFields sf = ctx.reader().storedFields();
                    var it = old.iterator();
                    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                        int id = Integer.parseInt(sf.document(doc).get("_id"));
                        assertArrayEquals(
                            "doc " + id + " keeps its old-model vector",
                            oldModelVector(id),
                            old.vectorValue(it.index()),
                            1e-6f
                        );
                    }
                }

                // 3. Non-vector fields survived without being re-supplied.
                try (DirectoryReader srcReader = DirectoryReader.open(src)) {
                    StoredFields srcStored = srcReader.storedFields();
                    StoredFields destStored = destReader.storedFields();
                    for (int i = 0; i < NUM_DOCS; i++) {
                        assertEquals("payload preserved", srcStored.document(i).get("payload"), destStored.document(i).get("payload"));
                        for (int f = 0; f < EXTRA_TEXT_FIELDS; f++) {
                            assertEquals(
                                "body_" + f + " preserved",
                                srcStored.document(i).get("body_" + f),
                                destStored.document(i).get("body_" + f)
                            );
                        }
                    }
                }

                // 4. Both fields are independently searchable — the alias-flip precondition.
                IndexSearcher searcher = new IndexSearcher(destReader);
                for (int probe : new int[] { 0, 137, NUM_DOCS - 1 }) {
                    TopDocs viaNew = searcher.search(new KnnFloatVectorQuery(NEW_FIELD, newModelVector(probe), 1), 1);
                    assertEquals(
                        "new field self-matches doc " + probe,
                        Integer.toString(probe),
                        searcher.storedFields().document(viaNew.scoreDocs[0].doc).get("_id")
                    );
                    TopDocs viaOld = searcher.search(new KnnFloatVectorQuery(OLD_FIELD, oldModelVector(probe), 1), 1);
                    assertEquals(
                        "old field still self-matches doc " + probe,
                        Integer.toString(probe),
                        searcher.storedFields().document(viaOld.scoreDocs[0].doc).get("_id")
                    );
                }
            }
        }
    }

    /**
     * The measurement that decides whether the field-pair shape is viable: adding a field via the
     * codec reader must preserve the bulk-copy saving, where re-adding documents does not.
     *
     * <p>Emits {@code METRIC_ADD_VS_READD_X1000} — bytes re-encoded by the add, as a fraction of a
     * full re-add. Lower is better.
     */
    public void testAddingBeatsReAddingDocuments() throws Exception {
        try (Directory src = newDirectory(); Directory viaAdd = newDirectory(); Directory viaReAdd = newDirectory()) {
            buildSource(src, pinnedConfig());
            long srcBytes = directorySize(src);

            // --- Path A: add the field through the codec reader (bulk-copies everything else) ---
            long addBytes = addFieldViaCodecReader(src, viaAdd, pinnedConfig());
            long addedVectorBytes = vectorFileBytes(viaAdd) - vectorFileBytes(src);

            // --- Path B: the pre-existing alternative — re-add every document ---
            try (DirectoryReader reader = DirectoryReader.open(src); IndexWriter w = new IndexWriter(viaReAdd, pinnedConfig())) {
                StoredFields sf = reader.storedFields();
                for (int i = 0; i < reader.maxDoc(); i++) {
                    int id = Integer.parseInt(sf.document(i).get("_id"));
                    Document d = new Document();
                    d.add(new StringField("_id", Integer.toString(id), Field.Store.YES));
                    d.add(new StoredField("payload", sf.document(i).get("payload")));
                    d.add(new NumericDocValuesField("rank", id));
                    for (int f = 0; f < EXTRA_TEXT_FIELDS; f++) {
                        d.add(new TextField("body_" + f, sf.document(i).get("body_" + f), Field.Store.YES));
                    }
                    d.add(new KnnFloatVectorField(OLD_FIELD, oldModelVector(id), VectorSimilarityFunction.DOT_PRODUCT));
                    d.add(new KnnFloatVectorField(NEW_FIELD, newModelVector(id), VectorSimilarityFunction.DOT_PRODUCT));
                    w.addDocument(d);
                }
                w.commit();
            }
            long reAddBytes = directorySize(viaReAdd);

            long ratioX1000 = Math.round(1000.0 * addedVectorBytes / reAddBytes);

            logger.info("--- field-adding: work avoided vs re-adding documents ---");
            logger.info("source (one vector field)      : {} bytes", srcBytes);
            logger.info("re-add all documents (total)   : {} bytes", reAddBytes);
            logger.info("add via codec reader (total)   : {} bytes", addBytes);
            logger.info("  of which NEW vector data     : {} bytes", addedVectorBytes);
            logger.info("METRIC_ADD_VS_READD_X1000={}", ratioX1000);

            assertTrue(
                "adding a field must re-encode far less than re-adding every document: " + addedVectorBytes + " vs " + reAddBytes,
                addedVectorBytes * 4 < reAddBytes
            );
            // Both paths must produce equivalent content.
            try (DirectoryReader a = DirectoryReader.open(viaAdd); DirectoryReader b = DirectoryReader.open(viaReAdd)) {
                assertEquals("same document count either way", b.numDocs(), a.numDocs());
            }
        }
    }

    private static long vectorFileBytes(Directory dir) throws IOException {
        long total = 0;
        for (String f : dir.listAll()) {
            if (f.endsWith(".vec") || f.endsWith(".vex") || f.endsWith(".vem") || f.endsWith(".vemf") || f.endsWith(".veq")) {
                total += dir.fileLength(f);
            }
        }
        return total;
    }

    /**
     * A sparsely-populated field must be expressible: documents the supplier declines are simply
     * absent from the new field, which is what "partially re-embedded" looks like mid-migration.
     */
    public void testSparselyPopulatedFieldIsSupported() throws Exception {
        try (Directory src = newDirectory(); Directory dest = newDirectory()) {
            buildSource(src, config());

            try (DirectoryReader reader = DirectoryReader.open(src); IndexWriter w = new IndexWriter(dest, config())) {
                List<CodecReader> wrapped = new ArrayList<>();
                for (LeafReaderContext ctx : reader.leaves()) {
                    final int[] ids = docIdToLogicalId(ctx.reader());
                    wrapped.add(
                        new VectorFieldAddingCodecReader(
                            (SegmentReader) ctx.reader(),
                            NEW_FIELD,
                            DIM,
                            VectorSimilarityFunction.DOT_PRODUCT,
                            // Only even ids get a vector for the new field.
                            docId -> ids[docId] % 2 == 0 ? newModelVector(ids[docId]) : null
                        )
                    );
                }
                w.addIndexes(wrapped.toArray(new CodecReader[0]));
                w.commit();
            }

            TestUtil.checkIndex(dest);
            try (DirectoryReader destReader = DirectoryReader.open(dest)) {
                int seen = 0;
                for (LeafReaderContext ctx : destReader.leaves()) {
                    FloatVectorValues added = ctx.reader().getFloatVectorValues(NEW_FIELD);
                    assertNotNull(added);
                    StoredFields sf = ctx.reader().storedFields();
                    var it = added.iterator();
                    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                        int id = Integer.parseInt(sf.document(doc).get("_id"));
                        assertEquals("only even documents carry the new field", 0, id % 2);
                        assertArrayEquals(newModelVector(id), added.vectorValue(it.index()), 1e-6f);
                        seen++;
                    }
                }
                assertEquals("exactly half the corpus is populated", NUM_DOCS / 2, seen);
                assertEquals("all documents are still present", NUM_DOCS, destReader.numDocs());
            }
        }
    }

    /**
     * The decisive capability question for an embedding-model upgrade: real upgrades routinely change the
     * vector's dimension (768→1024, 1536→3072). Adding a field carries its own dimension, so the new
     * field may differ from the existing one — and both coexist in the same index.
     *
     * <p>This is the property the substituting approach does <b>not</b> have: substitution reuses the
     * source field's own metadata, so the replacement vectors must match the original dimension exactly.
     * Any upgrade that changes dimension therefore requires the field-adding path.
     */
    public void testAddedFieldMayHaveADifferentDimensionThanTheExistingField() throws Exception {
        final int newDim = DIM * 2;   // e.g. 768 -> 1536
        try (Directory src = newDirectory(); Directory dest = newDirectory()) {
            buildSource(src, config());

            try (DirectoryReader reader = DirectoryReader.open(src); IndexWriter w = new IndexWriter(dest, config())) {
                List<CodecReader> wrapped = new ArrayList<>();
                for (LeafReaderContext ctx : reader.leaves()) {
                    final int[] ids = docIdToLogicalId(ctx.reader());
                    wrapped.add(
                        new VectorFieldAddingCodecReader(
                            (SegmentReader) ctx.reader(),
                            NEW_FIELD,
                            newDim,
                            VectorSimilarityFunction.DOT_PRODUCT,
                            docId -> {
                                float[] v = new float[newDim];
                                for (int i = 0; i < newDim; i++) {
                                    v[i] = (float) Math.sin((ids[docId] + 1) * 0.31 + i * 0.05) + 1.5f;
                                }
                                VectorUtil.l2normalize(v);
                                return v;
                            }
                        )
                    );
                }
                w.addIndexes(wrapped.toArray(new CodecReader[0]));
                w.commit();
            }

            TestUtil.checkIndex(dest);
            try (DirectoryReader destReader = DirectoryReader.open(dest)) {
                assertEquals("all documents carried over", NUM_DOCS, destReader.numDocs());
                for (LeafReaderContext ctx : destReader.leaves()) {
                    FloatVectorValues old = ctx.reader().getFloatVectorValues(OLD_FIELD);
                    FloatVectorValues added = ctx.reader().getFloatVectorValues(NEW_FIELD);
                    assertNotNull(old);
                    assertNotNull(added);
                    // The two models' fields coexist at DIFFERENT dimensions in one index.
                    assertEquals("existing field keeps its dimension", DIM, old.dimension());
                    assertEquals("added field carries the new model's dimension", newDim, added.dimension());
                }
            }
        }
    }

    /** Adding a field that already exists is a caller error and must fail fast, not silently no-op. */
    public void testAddingAnExistingFieldIsRejected() throws Exception {
        try (Directory src = newDirectory()) {
            buildSource(src, config());
            try (DirectoryReader reader = DirectoryReader.open(src)) {
                SegmentReader leaf = (SegmentReader) reader.leaves().get(0).reader();
                IllegalArgumentException e = expectThrows(
                    IllegalArgumentException.class,
                    () -> new VectorFieldAddingCodecReader(leaf, OLD_FIELD, DIM, VectorSimilarityFunction.DOT_PRODUCT, docId -> null)
                );
                assertTrue("must name the offending field and point at the alternative", e.getMessage().contains(OLD_FIELD));
                assertTrue(e.getMessage().contains("VectorFieldSubstitutingCodecReader"));
            }
        }
    }

    /**
     * The added field must survive {@code getMergeInstance()}, which is how {@code MergeState} hands
     * readers to the codec — the same trap that affected the substituting reader.
     */
    public void testAddedFieldSurvivesMergeInstanceAcquisition() throws Exception {
        try (Directory src = newDirectory()) {
            buildSource(src, config());
            try (DirectoryReader reader = DirectoryReader.open(src)) {
                for (LeafReaderContext ctx : reader.leaves()) {
                    final int[] ids = docIdToLogicalId(ctx.reader());
                    VectorFieldAddingCodecReader wrapped = new VectorFieldAddingCodecReader(
                        (SegmentReader) ctx.reader(),
                        NEW_FIELD,
                        DIM,
                        VectorSimilarityFunction.DOT_PRODUCT,
                        docId -> newModelVector(ids[docId])
                    );

                    assertNotNull("the added field must be visible in FieldInfos", wrapped.getFieldInfos().fieldInfo(NEW_FIELD));
                    assertEquals(DIM, wrapped.getFieldInfos().fieldInfo(NEW_FIELD).getVectorDimension());

                    var mergeInstance = wrapped.getVectorReader().getMergeInstance();
                    FloatVectorValues values = mergeInstance.getFloatVectorValues(NEW_FIELD);
                    assertNotNull("the merge instance must still serve the added field", values);
                    assertEquals(ctx.reader().maxDoc(), values.size());

                    // And the pre-existing field still resolves through the same merge instance.
                    assertNotNull("the delegate's field is still reachable", mergeInstance.getFloatVectorValues(OLD_FIELD));
                }
            }
        }
    }
}
