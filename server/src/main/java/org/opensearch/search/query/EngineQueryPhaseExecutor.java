package org.opensearch.search.query;

import org.opensearch.search.ContextEngineSearcher;
import org.opensearch.search.EngineReaderContext;
import org.opensearch.index.engine.ReadEngine;

/**
 * Generic engine query phase executor using ReadEngine
 */
public class EngineQueryPhaseExecutor implements QueryPhaseExecutor<EngineReaderContext> {

    @Override
    public boolean execute(EngineReaderContext context) throws QueryPhaseExecutionException {
//        ReadEngine<?, ?, ?, ?, ?> readEngine = context.indexShard()
//            .getIndexingExecutionCoordinator()
//            .getPrimaryReadEngine();
//
//        GenericQueryPhaseSearcher<?, ? ,?> searcher = readEngine.getQueryPhaseSearcher();
//        // TODO : figure out how to represent generic query object
//        GenericQueryPhase<?, ?, ?> queryPhase =
//            new GenericQueryPhase<>(searcher);
//
//        return queryPhase.executeInternal(context, context.contextEngineSearcher(), getQueryFromContext(context));

        ReadEngine<EngineReaderContext, ?, ?, ?, ContextEngineSearcher<?>> readEngine = context.indexShard()
            .getIndexingExecutionCoordinator()
            .getPrimaryReadEngine();

        if (readEngine == null) {
            throw new QueryPhaseExecutionException("Read engine is null");
        }

        GenericQueryPhaseSearcher<EngineReaderContext, ContextEngineSearcher<?>, ?> searcher =
            readEngine.getQueryPhaseSearcher();

        GenericQueryPhase<EngineReaderContext, ContextEngineSearcher<?>, ?> queryPhase =
            new GenericQueryPhase<>(searcher);

        Object query = getQueryFromContext(context);
        return queryPhase.executeInternal(context, context.contextEngineSearcher(), query);
    }

    @Override
    public boolean canHandle(EngineReaderContext context) {
        return context.indexShard()
            .getIndexingExecutionCoordinator()
            .getPrimaryReadEngine() != null;
    }

    private Object getQueryFromContext(EngineReaderContext context) {
        // Get query from context - could be Substrait bytes, Lucene Query, etc.
        // This would be part of the context interface
        return null;// For now, assuming Substrait
    }
}
