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

package org.opensearch.index.query;

import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.SetOnce;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public class RewriteableTests extends OpenSearchTestCase {

    public void testRewrite() throws IOException {
        QueryRewriteContext context = new QueryRewriteContext(null, null, null, null);
        TestRewriteable rewrite = Rewriteable.rewrite(
            new TestRewriteable(randomIntBetween(0, Rewriteable.MAX_REWRITE_ROUNDS)),
            context,
            randomBoolean()
        );
        assertEquals(rewrite.numRewrites, 0);
        IllegalStateException ise = expectThrows(
            IllegalStateException.class,
            () -> Rewriteable.rewrite(new TestRewriteable(Rewriteable.MAX_REWRITE_ROUNDS + 1), context)
        );
        assertEquals(ise.getMessage(), "too many rewrite rounds, rewriteable might return new objects even if they are not rewritten");
        ise = expectThrows(
            IllegalStateException.class,
            () -> Rewriteable.rewrite(new TestRewriteable(Rewriteable.MAX_REWRITE_ROUNDS + 1, true), context, true)
        );
        assertEquals(ise.getMessage(), "async actions are left after rewrite");
    }

    public void testRewriteAndFetch() throws ExecutionException, InterruptedException {
        QueryRewriteContext context = new QueryRewriteContext(null, null, null, null);
        PlainActionFuture<TestRewriteable> future = new PlainActionFuture<>();
        Rewriteable.rewriteAndFetch(new TestRewriteable(randomIntBetween(0, Rewriteable.MAX_REWRITE_ROUNDS), true), context, future);
        TestRewriteable rewrite = future.get();
        assertEquals(rewrite.numRewrites, 0);
        IllegalStateException ise = expectThrows(IllegalStateException.class, () -> {
            PlainActionFuture<TestRewriteable> f = new PlainActionFuture<>();
            Rewriteable.rewriteAndFetch(new TestRewriteable(Rewriteable.MAX_REWRITE_ROUNDS + 1, true), context, f);
            try {
                f.get();
            } catch (ExecutionException e) {
                throw e.getCause(); // we expect the underlying exception here
            }
        });
        assertEquals(ise.getMessage(), "too many rewrite rounds, rewriteable might return new objects even if they are not rewritten");
    }

    public void testRewriteList() throws IOException {
        QueryRewriteContext context = new QueryRewriteContext(null, null, null, null);
        List<TestRewriteable> rewriteableList = new ArrayList<>();
        int numInstances = randomIntBetween(1, 10);
        rewriteableList.add(new TestRewriteable(randomIntBetween(1, Rewriteable.MAX_REWRITE_ROUNDS)));
        for (int i = 0; i < numInstances; i++) {
            rewriteableList.add(new TestRewriteable(randomIntBetween(0, Rewriteable.MAX_REWRITE_ROUNDS)));
        }
        List<TestRewriteable> rewrittenList = Rewriteable.rewrite(rewriteableList, context);
        assertNotSame(rewrittenList, rewriteableList);
        for (TestRewriteable instance : rewrittenList) {
            assertEquals(0, instance.numRewrites);
        }
        rewriteableList = Collections.emptyList();
        assertSame(rewriteableList, Rewriteable.rewrite(rewriteableList, context));
        rewriteableList = null;
        assertNull(Rewriteable.rewrite(rewriteableList, context));

        rewriteableList = new ArrayList<>();
        for (int i = 0; i < numInstances; i++) {
            rewriteableList.add(new TestRewriteable(0));
        }
        assertSame(rewriteableList, Rewriteable.rewrite(rewriteableList, context));
    }

    private static final class TestRewriteable implements Rewriteable<TestRewriteable> {

        final int numRewrites;
        final boolean fetch;
        final Supplier<Boolean> supplier;

        TestRewriteable(int numRewrites) {
            this(numRewrites, false, null);
        }

        TestRewriteable(int numRewrites, boolean fetch) {
            this(numRewrites, fetch, null);
        }

        TestRewriteable(int numRewrites, boolean fetch, Supplier<Boolean> supplier) {
            this.numRewrites = numRewrites;
            this.fetch = fetch;
            this.supplier = supplier;
        }

        @Override
        public TestRewriteable rewrite(QueryRewriteContext ctx) throws IOException {
            if (numRewrites == 0) {
                return this;
            }
            if (supplier != null && supplier.get() == null) {
                return this;
            }
            if (supplier != null) {
                assertTrue(supplier.get());
            }
            if (fetch) {
                SetOnce<Boolean> setOnce = new SetOnce<>();
                ctx.registerAsyncAction((c, l) -> {
                    Runnable r = () -> {
                        setOnce.set(Boolean.TRUE);
                        l.onResponse(null);
                    };
                    if (randomBoolean()) {
                        new Thread(r).start();
                    } else {
                        r.run();
                    }
                });
                return new TestRewriteable(numRewrites - 1, fetch, setOnce::get);
            }
            return new TestRewriteable(numRewrites - 1, fetch, null);
        }
    }
}
