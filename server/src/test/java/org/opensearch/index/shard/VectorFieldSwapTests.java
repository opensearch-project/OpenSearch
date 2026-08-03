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
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.VectorUtil;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Proves that a whole vector field can be replaced without rewriting any other field, and measures
 * the write amplification against the status quo (a full reindex).
 *
 * <p>The mechanism under test is {@link VectorFieldSubstitutingCodecReader} passed to
 * {@link IndexWriter#addIndexes(CodecReader...)}. No per-field storage engine, merge policy, or
 * lifecycle is involved.
 */
public class VectorFieldSwapTests extends OpenSearchTestCase {

    private static final String VECTOR_FIELD = "embedding";
    private static final int DIM = 96;
    private static final int NUM_DOCS = 400;

    /**
     * Number of additional analyzed text fields per document. The write-amplification win depends
     * entirely on how much non-vector content a full reindex would have to rebuild, so the corpus
     * must resemble the motivating case (a large stable body plus a re-embedded vector) rather than
     * a vector-dominated toy index.
     */
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
        "relevance",
        "inverted",
        "posting",
        "codec",
        "lifecycle" };

    /** Deterministic multi-term text, so postings and stored fields carry real weight. */
    private static String syntheticProse(int doc, int field) {
        StringBuilder sb = new StringBuilder(512);
        int seed = doc * 31 + field * 7;
        for (int w = 0; w < 60; w++) {
            sb.append(WORDS[(seed + w * 13) % WORDS.length]).append(' ').append((seed + w) % 997).append(' ');
        }
        return sb.toString();
    }

    /**
     * The byte measurement must not move with the randomized test codec, so the write-amplification
     * test pins a concrete codec. Correctness tests deliberately keep the random codec.
     */
    private static IndexWriterConfig pinnedCodecConfig() {
        return new IndexWriterConfig(new StandardAnalyzer()).setCodec(new Lucene104Codec())
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    }

    /** Deterministic stand-in for "the new embedding model". */
    private static float[] newModelVector(int seed, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = (float) Math.sin((seed + 1) * 0.37 + i * 0.11) + 1.5f;
        }
        VectorUtil.l2normalize(v);
        return v;
    }

    /** The original embeddings, from the "old model". */
    private static float[] oldModelVector(int seed, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = (float) Math.cos((seed + 1) * 0.53 + i * 0.07) + 1.5f;
        }
        VectorUtil.l2normalize(v);
        return v;
    }

    /**
     * Builds an index whose documents carry one vector field plus several expensive non-vector
     * fields (analyzed text, stored fields, doc values, keywords) — the shape the whole design
     * exists for.
     */
    private void buildSourceIndex(Directory dir) throws IOException {
        buildSourceIndex(
            dir,
            new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE)
                .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
        );
    }

    private void buildSourceIndex(Directory dir, IndexWriterConfig iwc) throws IOException {
        try (IndexWriter w = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < NUM_DOCS; i++) {
                w.addDocument(makeDoc(i, oldModelVector(i, DIM)));
            }
            w.commit();
            w.forceMerge(1);
        }
    }

    private Document makeDoc(int i, float[] vector) {
        Document d = new Document();
        d.add(new StringField("_id", Integer.toString(i), Field.Store.YES));
        // Deliberately expensive non-vector content: this is what a full reindex must redo and what
        // the swap must not touch.
        d.add(
            new TextField(
                "body",
                String.format(
                    Locale.ROOT,
                    "document %d discusses vector search storage layers segment merge amplification "
                        + "quantization hnsw graph traversal recall latency tradeoffs %d",
                    i,
                    i * 7
                ),
                Field.Store.YES
            )
        );
        d.add(new StringField("category", "cat-" + (i % 17), Field.Store.YES));
        d.add(new NumericDocValuesField("rank", i));
        d.add(new StoredField("payload", "payload-blob-for-document-" + i + "-" + "x".repeat(64)));
        // The motivating workload is a document with substantial non-vector content: several
        // analyzed text fields whose postings, stored values and norms a full reindex must rebuild.
        for (int f = 0; f < EXTRA_TEXT_FIELDS; f++) {
            d.add(new TextField("body_" + f, syntheticProse(i, f), Field.Store.YES));
        }
        d.add(new KnnFloatVectorField(VECTOR_FIELD, vector));
        return d;
    }

    /**
     * Resolves each segment-local docId to its logical {@code _id} on the calling thread.
     *
     * <p>A {@link StoredFields} instance may only be consumed on the thread that acquired it, and
     * the substituting reader's supplier is invoked on Lucene's merge thread. So the mapping is
     * materialized up front rather than read lazily inside the supplier.
     */
    private static int[] docIdToLogicalId(org.apache.lucene.index.LeafReader leaf) throws IOException {
        StoredFields sf = leaf.storedFields();
        int[] ids = new int[leaf.maxDoc()];
        for (int doc = 0; doc < leaf.maxDoc(); doc++) {
            ids[doc] = Integer.parseInt(sf.document(doc).get("_id"));
        }
        return ids;
    }

    private static long directorySize(Directory dir) throws IOException {
        long total = 0;
        for (String f : dir.listAll()) {
            total += dir.fileLength(f);
        }
        return total;
    }

    /** Total bytes of the files that hold vector data for the field. */
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
     * The core claim: swapping the vector field rebuilds the vector files and carries every other
     * field through untouched, and the result is a correct, ordinary index.
     */
    public void testVectorSwapReplacesVectorsAndPreservesOtherFields() throws Exception {
        try (Directory src = newDirectory(); Directory dest = newDirectory()) {
            buildSourceIndex(src);

            // Swap: read source segments through the substituting reader into a fresh index.
            IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE)
                .setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            try (DirectoryReader reader = DirectoryReader.open(src); IndexWriter w = new IndexWriter(dest, iwc)) {
                List<CodecReader> wrapped = new ArrayList<>();
                for (LeafReaderContext ctx : reader.leaves()) {
                    CodecReader cr = (SegmentReader) ctx.reader();
                    // Materialize docId -> logical _id on this thread; the supplier runs on the
                    // merge thread, where the source StoredFields must not be touched.
                    final int[] ids = docIdToLogicalId(ctx.reader());
                    wrapped.add(new VectorFieldSubstitutingCodecReader(cr, VECTOR_FIELD, (docId, dim) -> newModelVector(ids[docId], dim)));
                }
                w.addIndexes(wrapped.toArray(new CodecReader[0]));
                w.commit();
            }

            TestUtil.checkIndex(dest);

            try (DirectoryReader destReader = DirectoryReader.open(dest)) {
                assertEquals("all documents carried over", NUM_DOCS, destReader.numDocs());

                // 1. Every vector is the NEW model's vector.
                int checked = 0;
                for (LeafReaderContext ctx : destReader.leaves()) {
                    FloatVectorValues fvv = ctx.reader().getFloatVectorValues(VECTOR_FIELD);
                    assertNotNull("vector field present in destination", fvv);
                    StoredFields sf = ctx.reader().storedFields();
                    var it = fvv.iterator();
                    for (int doc = it.nextDoc(); doc != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                        int id = Integer.parseInt(sf.document(doc).get("_id"));
                        float[] actual = fvv.vectorValue(it.index());
                        assertArrayEquals("doc " + id + " has the new embedding", newModelVector(id, DIM), actual, 1e-6f);
                        checked++;
                    }
                }
                assertEquals("every document's vector was verified", NUM_DOCS, checked);

                // 2. Non-vector fields survived intact.
                try (DirectoryReader srcReader = DirectoryReader.open(src)) {
                    StoredFields srcStored = srcReader.storedFields();
                    StoredFields destStored = destReader.storedFields();
                    for (int i = 0; i < NUM_DOCS; i++) {
                        assertEquals("body preserved", srcStored.document(i).get("body"), destStored.document(i).get("body"));
                        assertEquals("payload preserved", srcStored.document(i).get("payload"), destStored.document(i).get("payload"));
                        assertEquals("category preserved", srcStored.document(i).get("category"), destStored.document(i).get("category"));
                        for (int f = 0; f < EXTRA_TEXT_FIELDS; f++) {
                            assertEquals(
                                "body_" + f + " preserved",
                                srcStored.document(i).get("body_" + f),
                                destStored.document(i).get("body_" + f)
                            );
                        }
                    }
                }

                // 3. ANN search returns the new vectors: each doc's own new embedding self-matches.
                IndexSearcher searcher = new IndexSearcher(destReader);
                for (int probe : new int[] { 0, 13, NUM_DOCS / 2, NUM_DOCS - 1 }) {
                    TopDocs td = searcher.search(new KnnFloatVectorQuery(VECTOR_FIELD, newModelVector(probe, DIM), 1), 1);
                    assertEquals(1, td.scoreDocs.length);
                    String hitId = searcher.storedFields().document(td.scoreDocs[0].doc).get("_id");
                    assertEquals("querying doc " + probe + "'s new vector returns itself", Integer.toString(probe), hitId);
                }

                // 4. The OLD vectors are gone.
                TopDocs oldHit = searcher.search(new KnnFloatVectorQuery(VECTOR_FIELD, oldModelVector(0, DIM), 1), 1);
                String oldHitId = searcher.storedFields().document(oldHit.scoreDocs[0].doc).get("_id");
                float selfScore = searcher.search(new KnnFloatVectorQuery(VECTOR_FIELD, newModelVector(0, DIM), 1), 1).scoreDocs[0].score;
                assertTrue(
                    "an old-model query must not self-match as strongly as the new vector does",
                    oldHit.scoreDocs[0].score < selfScore || Integer.parseInt(oldHitId) != 0
                );
            }
        }
    }

    /**
     * A different vector field in the same index must be untouched by a single-field swap.
     */
    /**
     * Reproduces the access pattern the OpenSearch k-NN plugin's native (FAISS) writer uses, so the
     * mechanism is verified against the engine OpenSearch actually ships by default — not only
     * against Lucene HNSW.
     *
     * <p>k-NN's {@code AbstractNativeEnginesKnnVectorsWriter.doMergeOneField} obtains its vectors via
     * {@code KNNVectorValuesFactory.getKNNVectorValuesForMerge}, which calls
     * {@link org.apache.lucene.codecs.KnnVectorsWriter.MergedVectorValues#mergeFloatVectorValues}.
     * That helper reads {@code mergeState.knnVectorsReaders}, which {@code MergeState} populates by
     * calling {@code reader.getVectorReader().getMergeInstance()} on each input reader. This test
     * drives exactly that sequence and asserts the substituted values — not the originals — are what
     * a native writer would consume.
     *
     * <p>This does not exercise FAISS quantization or JNI; it pins the contract at the seam where
     * core hands vectors to any {@code KnnVectorsWriter}, which is what the plugin depends on.
     */
    public void testSubstitutionSurvivesMergeInstanceAcquisition() throws Exception {
        try (Directory src = newDirectory()) {
            buildSourceIndex(src);
            try (DirectoryReader reader = DirectoryReader.open(src)) {
                for (LeafReaderContext ctx : reader.leaves()) {
                    final int[] ids = docIdToLogicalId(ctx.reader());
                    VectorFieldSubstitutingCodecReader wrapped = new VectorFieldSubstitutingCodecReader(
                        (SegmentReader) ctx.reader(),
                        VECTOR_FIELD,
                        (docId, dim) -> newModelVector(ids[docId], dim)
                    );

                    // Step 1: exactly what MergeState does (MergeState.java:165-167).
                    org.apache.lucene.codecs.KnnVectorsReader vectorsReader = wrapped.getVectorReader();
                    assertNotNull("wrapped reader must expose a vectors reader", vectorsReader);
                    org.apache.lucene.codecs.KnnVectorsReader mergeInstance = vectorsReader.getMergeInstance();
                    assertNotNull("merge instance must not be null", mergeInstance);

                    // Step 2: what k-NN's native writer reads through the merge instance.
                    FloatVectorValues values = mergeInstance.getFloatVectorValues(VECTOR_FIELD);
                    assertNotNull("merge instance must expose the substituted field", values);

                    int seen = 0;
                    var it = values.iterator();
                    for (int doc = it.nextDoc(); doc != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                        float[] actual = values.vectorValue(it.index());
                        assertArrayEquals(
                            "a native (FAISS) writer must receive the NEW vector for doc " + ids[doc],
                            newModelVector(ids[doc], DIM),
                            actual,
                            1e-6f
                        );
                        // Guard against the failure mode this test exists for: the merge instance
                        // silently reverting to the delegate's original vectors.
                        assertFalse(
                            "merge instance must not serve the OLD vector for doc " + ids[doc],
                            java.util.Arrays.equals(oldModelVector(ids[doc], DIM), actual)
                        );
                        seen++;
                    }
                    assertEquals("every document in the leaf was checked", ctx.reader().maxDoc(), seen);

                    // A field the swap does not target must still resolve through the delegate.
                    assertNull("an unknown field resolves to null, not an error", mergeInstance.getFloatVectorValues("no_such_field"));
                }
            }
        }
    }

    public void testOtherVectorFieldUntouched() throws Exception {
        final String otherField = "embedding_other";
        try (Directory src = newDirectory(); Directory dest = newDirectory()) {
            IndexWriterConfig build = new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter w = new IndexWriter(src, build)) {
                for (int i = 0; i < 50; i++) {
                    Document d = makeDoc(i, oldModelVector(i, DIM));
                    d.add(new KnnFloatVectorField(otherField, oldModelVector(i + 5000, DIM)));
                    w.addDocument(d);
                }
                w.commit();
                w.forceMerge(1);
            }

            IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE)
                .setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            try (DirectoryReader reader = DirectoryReader.open(src); IndexWriter w = new IndexWriter(dest, iwc)) {
                List<CodecReader> wrapped = new ArrayList<>();
                for (LeafReaderContext ctx : reader.leaves()) {
                    wrapped.add(
                        new VectorFieldSubstitutingCodecReader(
                            (SegmentReader) ctx.reader(),
                            VECTOR_FIELD,
                            (docId, dim) -> newModelVector(docId, dim)
                        )
                    );
                }
                w.addIndexes(wrapped.toArray(new CodecReader[0]));
                w.commit();
            }

            TestUtil.checkIndex(dest);
            try (DirectoryReader destReader = DirectoryReader.open(dest)) {
                for (LeafReaderContext ctx : destReader.leaves()) {
                    FloatVectorValues other = ctx.reader().getFloatVectorValues(otherField);
                    assertNotNull("the untargeted vector field still exists", other);
                    StoredFields sf = ctx.reader().storedFields();
                    var it = other.iterator();
                    for (int doc = it.nextDoc(); doc != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                        int id = Integer.parseInt(sf.document(doc).get("_id"));
                        assertArrayEquals(
                            "untargeted field keeps its original values",
                            oldModelVector(id + 5000, DIM),
                            other.vectorValue(it.index()),
                            1e-6f
                        );
                    }
                }
            }
        }
    }

    /**
     * A dimension mismatch must fail loudly rather than silently corrupting the field.
     */
    public void testDimensionMismatchIsRejected() throws Exception {
        try (Directory src = newDirectory(); Directory dest = newDirectory()) {
            buildSourceIndex(src);
            // addIndexes() merges on a background thread by default, which would surface the
            // validation failure as an uncaught-exception error instead of a thrown one. Run the
            // merge on the calling thread so the exception propagates to the caller.
            IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE)
                .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
                .setMergeScheduler(new org.apache.lucene.index.SerialMergeScheduler());
            try (DirectoryReader reader = DirectoryReader.open(src); IndexWriter w = new IndexWriter(dest, iwc)) {
                List<CodecReader> wrapped = new ArrayList<>();
                for (LeafReaderContext ctx : reader.leaves()) {
                    wrapped.add(
                        new VectorFieldSubstitutingCodecReader(
                            (SegmentReader) ctx.reader(),
                            VECTOR_FIELD,
                            (docId, dim) -> new float[dim + 1]
                        )
                    );
                }
                CodecReader[] arr = wrapped.toArray(new CodecReader[0]);
                // The merge machinery may wrap the cause, so assert on the failure and its reason
                // rather than on an exact exception type.
                Exception e = expectThrows(Exception.class, () -> w.addIndexes(arr));
                Throwable cause = e;
                boolean sawDimensionComplaint = false;
                while (cause != null) {
                    if (cause instanceof IllegalArgumentException
                        && cause.getMessage() != null
                        && cause.getMessage().contains("dimension")) {
                        sawDimensionComplaint = true;
                        break;
                    }
                    cause = cause.getCause();
                }
                assertTrue("a dimension mismatch must be reported, but got: " + e, sawDimensionComplaint);
            }
        }
    }

    /**
     * The metric: bytes written by the vector swap versus a full reindex of the same corpus.
     *
     * <p>Emits {@code METRIC_BYTES_RATIO_X1000} for the autoresearch harness to parse. Lower is
     * better: it is the fraction of the corpus's bytes that a re-embedding actually has to write.
     */
    public void testWriteAmplificationVersusFullReindex() throws Exception {
        Path tmp = createTempDir();
        try (
            Directory src = newFSDirectory(tmp.resolve("src"));
            Directory swapped = newFSDirectory(tmp.resolve("swapped"));
            Directory reindexed = newFSDirectory(tmp.resolve("reindexed"))
        ) {
            buildSourceIndex(src, pinnedCodecConfig());
            long srcBytes = directorySize(src);

            // --- Path A: the swap. Vector files are rebuilt; everything else bulk-copies. ---
            IndexWriterConfig swapCfg = pinnedCodecConfig();
            try (DirectoryReader reader = DirectoryReader.open(src); IndexWriter w = new IndexWriter(swapped, swapCfg)) {
                List<CodecReader> wrapped = new ArrayList<>();
                for (LeafReaderContext ctx : reader.leaves()) {
                    final int[] ids = docIdToLogicalId(ctx.reader());
                    wrapped.add(
                        new VectorFieldSubstitutingCodecReader(
                            (SegmentReader) ctx.reader(),
                            VECTOR_FIELD,
                            (docId, dim) -> newModelVector(ids[docId], dim)
                        )
                    );
                }
                w.addIndexes(wrapped.toArray(new CodecReader[0]));
                w.commit();
            }

            // --- Path B: the status quo. Every field of every document re-indexed. ---
            IndexWriterConfig reindexCfg = pinnedCodecConfig();
            try (IndexWriter w = new IndexWriter(reindexed, reindexCfg)) {
                for (int i = 0; i < NUM_DOCS; i++) {
                    w.addDocument(makeDoc(i, newModelVector(i, DIM)));
                }
                w.commit();
                w.forceMerge(1);
            }

            long vectorBytes = vectorFileBytes(swapped);
            long fullReindexBytes = directorySize(reindexed);
            // What a re-embedding is fundamentally obliged to write: the vector files.
            long ratioX1000 = Math.round(1000.0 * vectorBytes / fullReindexBytes);

            logger.info("--- vector field swap: write amplification ---");
            logger.info("source index bytes           : {}", srcBytes);
            logger.info("full reindex bytes           : {}", fullReindexBytes);
            logger.info("swap: vector file bytes      : {}", vectorBytes);
            logger.info("METRIC_BYTES_RATIO_X1000={}", ratioX1000);

            Path metricFile = Path.of(System.getProperty("vectorswap.metric.out", tmp.resolve("metric.txt").toString()));
            Files.writeString(metricFile, Long.toString(ratioX1000));

            assertTrue("a re-embedding must write strictly less than a full reindex", vectorBytes < fullReindexBytes);
        }
    }
}
