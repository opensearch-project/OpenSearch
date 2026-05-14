package org.opensearch.be.lucene.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.be.lucene.LuceneDataFormat;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DeleteExecutionEngine;
import org.opensearch.index.engine.dataformat.DeleteInput;
import org.opensearch.index.engine.dataformat.DeleteResult;
import org.opensearch.index.engine.dataformat.Deleter;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.dataformat.DeleterImpl;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.opensearch.be.lucene.index.LuceneWriter.WRITER_GENERATION_ATTRIBUTE;

/**
 * Lucene-based implementation of {@link DeleteExecutionEngine} that tracks per-generation
 * deleters paired with their corresponding writers. Each deleter delegates document
 * deletion to the underlying {@link LuceneWriter}.
 *
 * @opensearch.experimental
 */
public class LuceneDeleteExecutionEngine implements DeleteExecutionEngine<DataFormat> {

    private static final Logger logger = LogManager.getLogger(LuceneDeleteExecutionEngine.class);

    private final Map<Long, Deleter> generationToDeleterMap;
    private final DataFormat dataFormat;
    private final LuceneCommitter committer;

    public LuceneDeleteExecutionEngine(DataFormat dataFormat, Committer committer) {
        this.generationToDeleterMap = new ConcurrentHashMap<>();
        this.dataFormat = dataFormat;
        this.committer = (LuceneCommitter) committer;
    }

    @Override
    public Deleter createDeleter(Writer<?> writer) {
        Optional<? extends Writer<?>> luceneWriterOpt = writer.getWriterForFormat(LuceneDataFormat.LUCENE_FORMAT_NAME);
        if (luceneWriterOpt.isEmpty()) {
            // Parquet-only writer (no per-gen Lucene writer to pair with).
            // deleteDocument falls back to the shared committer's IndexWriter for this generation.
            return null;
        }
        LuceneWriter luceneWriter = (LuceneWriter) luceneWriterOpt.get();
        Deleter deleter = new DeleterImpl<>(luceneWriter);
        generationToDeleterMap.put(writer.generation(), deleter);
        return deleter;
    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) throws IOException {
        return null;
    }

    @Override
    public DeleteResult deleteDocument(DeleteInput deleteInput) throws IOException {
        Deleter deleter = generationToDeleterMap.get(deleteInput.generation());
        if (deleter != null) {
            return deleter.deleteDoc(deleteInput);
        } else {
            Term uid = new Term(deleteInput.fieldName(), deleteInput.value());
            this.committer.getIndexWriter().deleteDocuments(uid);
            return new DeleteResult.Success(1L, 1L, 1L);
        }
    }

    @Override
    public DataFormat getDataFormat() {
        return this.dataFormat;
    }

    @Override
    public Map<Long, long[]> getLiveDocsForSegments(List<Segment> segments) throws IOException {
        Objects.requireNonNull(segments, "segments");
        Map<Long, long[]> result = new HashMap<>();
        if (segments.isEmpty()) {
            return result;
        }
        Map<Long, Segment> requested = new HashMap<>(segments.size());
        for (Segment seg : segments) {
            requested.put(seg.generation(), seg);
        }
        try (DirectoryReader reader = DirectoryReader.open(committer.getIndexWriter())) {
            for (LeafReaderContext lrc : reader.leaves()) {
                LeafReader leaf = lrc.reader();
                if ((leaf instanceof SegmentReader) == false) {
                    continue;
                }
                SegmentCommitInfo sci = ((SegmentReader) leaf).getSegmentInfo();
                String attr = sci.info.getAttribute(WRITER_GENERATION_ATTRIBUTE);
                if (attr == null) {
                    continue;
                }
                long generation = Long.parseLong(attr);
                if (requested.containsKey(generation) == false) {
                    continue;
                }
                Bits liveDocs = leaf.getLiveDocs();
                if (liveDocs == null) {
                    continue;
                }
                result.put(generation, packLiveDocs(liveDocs, leaf.maxDoc()));
            }
        }
        return result;
    }

    private static long[] packLiveDocs(Bits liveDocs, int numRows) {
        FixedBitSet bitSet = new FixedBitSet(numRows);
        for (int i = 0; i < numRows; i++) {
            if (liveDocs.get(i)) {
                bitSet.set(i);
            }
        }
        return bitSet.getBits();
    }

    @Override
    public void close() throws IOException {
        for (Deleter deleter : generationToDeleterMap.values()) {
            deleter.close();
        }
        generationToDeleterMap.clear();
    }
}
