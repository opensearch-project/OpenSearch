package org.opensearch.be.lucene.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.Term;
import org.opensearch.be.lucene.LuceneDataFormat;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DeleteExecutionEngine;
import org.opensearch.index.engine.dataformat.DeleteInput;
import org.opensearch.index.engine.dataformat.DeleteResult;
import org.opensearch.index.engine.dataformat.Deleter;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.dataformat.DeleterImpl;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
        LuceneWriter luceneWriter = writer.getWriterForFormat(LuceneDataFormat.LUCENE_FORMAT_NAME)
            .map(w -> (LuceneWriter) w)
            .orElseThrow(
                () -> new IllegalArgumentException("Cannot create deleter: no Lucene writer found for generation=" + writer.generation())
            );
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
    public void close() throws IOException {
        for (Deleter deleter : generationToDeleterMap.values()) {
            deleter.close();
        }
        generationToDeleterMap.clear();
    }
}
