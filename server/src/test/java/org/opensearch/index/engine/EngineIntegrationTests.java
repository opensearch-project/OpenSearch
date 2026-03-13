/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.search.SearchExecutionContext;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

public class EngineIntegrationTests extends OpenSearchTestCase {

    public void testEngineSearcherSearchIsCalled() throws IOException {
        boolean[] called = { false };
        StubEngine engine = new StubEngine() {
            @Override
            public void execute(Ctx c) throws IOException {
                EngineSearcher<?> s = acquireSearcher("t");
                ((StubSearcher) s).search(c);
            }

            @Override
            public EngineSearcherSupplier<StubSearcher> acquireSearcherSupplier() {
                return new EngineSearcherSupplier<>() {
                    @Override
                    protected StubSearcher acquireSearcherInternal(String src) {
                        return new StubSearcher() {
                            @Override
                            public void search(Ctx c) {
                                called[0] = true;
                                c.executed = true;
                            }
                        };
                    }

                    @Override
                    protected void doClose() {}
                };
            }
        };
        Ctx ctx = new Ctx();
        engine.execute(ctx);
        assertTrue(called[0]);
        assertTrue(ctx.executed);
    }

    public void testReaderManagerAcquireRelease() throws IOException {
        int[] rc = { 0 };
        EngineReaderManager<String> rm = new EngineReaderManager<>() {
            @Override
            public String acquire() {
                rc[0]++;
                return "snap";
            }

            @Override
            public void release(String r) {
                rc[0]--;
            }
        };
        assertEquals("snap", rm.acquire());
        rm.release("snap");
        assertEquals(0, rc[0]);
    }

    public void testCompositeEngineRegistersAndCloses() throws IOException {
        boolean[] closed = { false };
        StubEngine stub = new StubEngine() {
            @Override
            public void close() {
                closed[0] = true;
            }
        };
        @SuppressWarnings("unchecked")
        org.opensearch.plugins.SearchEnginePlugin plugin = p -> (SearchExecEngine) stub;
        CompositeEngine ce = new CompositeEngine(List.of(plugin), null);
        assertTrue(ce.getReadEngines().size() > 0);
        assertNotNull(ce.getPrimaryReadEngine());
        ce.close();
        assertTrue(closed[0]);
    }

    public void testIndexFilterTreeIsCloseable() throws IOException {
        new IndexFilterTree().close();
    }

    static class Ctx implements SearchExecutionContext {
        boolean executed;

        @Override
        public ShardSearchContextId id() {
            return null;
        }

        @Override
        public ShardSearchRequest request() {
            return null;
        }

        @Override
        public SearchShardTarget shardTarget() {
            return null;
        }

        @Override
        public QuerySearchResult queryResult() {
            return null;
        }

        @Override
        public FetchSearchResult fetchResult() {
            return null;
        }

        @Override
        public int from() {
            return 0;
        }

        @Override
        public int size() {
            return 10;
        }

        @Override
        public int[] docIdsToLoad() {
            return null;
        }

        @Override
        public void docIdsToLoad(int[] d, int f, int s) {}

        @Override
        public void close() {}
    }

    static class StubSearcher implements EngineSearcher<Ctx> {
        @Override
        public String source() {
            return "stub";
        }

        @Override
        public void search(Ctx c) {}

        @Override
        public void close() {}
    }

    static class StubEngine implements SearchExecEngine<Ctx, String> {
        @Override
        public void execute(Ctx c) throws IOException {}

        @Override
        public Ctx createContext(
            org.opensearch.search.internal.ReaderContext rc,
            org.opensearch.search.internal.ShardSearchRequest req,
            org.opensearch.search.SearchShardTarget st,
            org.opensearch.action.search.SearchShardTask t,
            org.opensearch.common.util.BigArrays ba
        ) {
            return new Ctx();
        }

        @Override
        public EngineSearcherSupplier<?> acquireSearcherSupplier() {
            return null;
        }

        @Override
        public EngineReaderManager<?> getReferenceManager() {
            return null;
        }
    }
}
