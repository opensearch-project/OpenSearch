/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.core.action.ActionListener;
import org.opensearch.search.SearchExecutionContext;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class SearchExecEngineTests extends OpenSearchTestCase {

    public void testAsyncExecuteCallsOnResponseOnSuccess() {
        Ctx ctx = new Ctx();
        MockEngine engine = new MockEngine(c -> {});
        AtomicReference<Ctx> ref = new AtomicReference<>();
        engine.execute(ctx, new ActionListener<>() {
            @Override
            public void onResponse(Ctx r) {
                ref.set(r);
            }

            @Override
            public void onFailure(Exception e) {
                fail();
            }
        });
        assertSame(ctx, ref.get());
    }

    public void testAsyncExecuteCallsOnFailureOnIOException() {
        Ctx ctx = new Ctx();
        IOException expected = new IOException("simulated");
        MockEngine engine = new MockEngine(c -> { throw expected; });
        AtomicReference<Exception> ref = new AtomicReference<>();
        engine.execute(ctx, new ActionListener<>() {
            @Override
            public void onResponse(Ctx r) {
                fail();
            }

            @Override
            public void onFailure(Exception e) {
                ref.set(e);
            }
        });
        assertSame(expected, ref.get());
    }

    public void testAsyncExecuteCallsOnFailureOnRuntimeException() {
        Ctx ctx = new Ctx();
        RuntimeException expected = new RuntimeException("simulated");
        MockEngine engine = new MockEngine(c -> { throw expected; });
        AtomicReference<Exception> ref = new AtomicReference<>();
        engine.execute(ctx, new ActionListener<>() {
            @Override
            public void onResponse(Ctx r) {
                fail();
            }

            @Override
            public void onFailure(Exception e) {
                ref.set(e);
            }
        });
        assertSame(expected, ref.get());
    }

    public void testDqeBridgeDefaultsThrow() {
        MockEngine engine = new MockEngine(c -> {});
        expectThrows(UnsupportedOperationException.class, () -> engine.convertFragment("x"));
        expectThrows(UnsupportedOperationException.class, () -> engine.executePlan("p", new Ctx()));
    }

    @FunctionalInterface
    interface Exec {
        void run(Ctx c) throws IOException;
    }

    static class MockEngine implements SearchExecEngine<Ctx, String> {
        private final Exec fn;

        MockEngine(Exec fn) {
            this.fn = fn;
        }

        @Override
        public void execute(Ctx c) throws IOException {
            fn.run(c);
        }

        @Override
        public Ctx createContext(
            ReaderContext rc,
            ShardSearchRequest req,
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

    static class Ctx implements SearchExecutionContext {
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
            return 0;
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
}
