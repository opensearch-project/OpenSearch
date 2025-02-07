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

package org.opensearch.transport.client.node;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionType;
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteAction;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.opensearch.action.admin.cluster.stats.ClusterStatsAction;
import org.opensearch.action.admin.cluster.storedscripts.DeleteStoredScriptAction;
import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheAction;
import org.opensearch.action.admin.indices.create.CreateIndexAction;
import org.opensearch.action.admin.indices.flush.FlushAction;
import org.opensearch.action.admin.indices.stats.IndicesStatsAction;
import org.opensearch.action.delete.DeleteAction;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.index.IndexAction;
import org.opensearch.action.search.SearchAction;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.env.Environment;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public abstract class AbstractClientHeadersTestCase extends OpenSearchTestCase {

    protected static final Settings HEADER_SETTINGS = Settings.builder()
        .put(ThreadContext.PREFIX + ".key1", "val1")
        .put(ThreadContext.PREFIX + ".key2", "val 2")
        .build();

    private static final ActionType<?>[] ACTIONS = new ActionType[] {
        // client actions
        GetAction.INSTANCE,
        SearchAction.INSTANCE,
        DeleteAction.INSTANCE,
        DeleteStoredScriptAction.INSTANCE,
        IndexAction.INSTANCE,

        // cluster admin actions
        ClusterStatsAction.INSTANCE,
        CreateSnapshotAction.INSTANCE,
        ClusterRerouteAction.INSTANCE,

        // indices admin actions
        CreateIndexAction.INSTANCE,
        IndicesStatsAction.INSTANCE,
        ClearIndicesCacheAction.INSTANCE,
        FlushAction.INSTANCE };

    protected ThreadPool threadPool;
    private Client client;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Settings settings = Settings.builder()
            .put(HEADER_SETTINGS)
            .put("path.home", createTempDir().toString())
            .put("node.name", "test-" + getTestName())
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        threadPool = new ThreadPool(settings);
        client = buildClient(settings, ACTIONS);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        client.close();
        terminate(threadPool);
    }

    protected abstract Client buildClient(Settings headersSettings, ActionType<?>[] testedActions);

    public void testActions() {

        // TODO this is a really shitty way to test it, we need to figure out a way to test all the client methods
        // without specifying each one (reflection doesn't as each action needs its own special settings, without
        // them, request validation will fail before the test is executed. (one option is to enable disabling the
        // validation in the settings??? - ugly and conceptually wrong)

        // choosing arbitrary top level actions to test
        client.prepareGet("idx", "id").execute(new AssertingActionListener<>(GetAction.NAME, client.threadPool()));
        client.prepareSearch().execute(new AssertingActionListener<>(SearchAction.NAME, client.threadPool()));
        client.prepareDelete("idx", "id").execute(new AssertingActionListener<>(DeleteAction.NAME, client.threadPool()));
        client.admin()
            .cluster()
            .prepareDeleteStoredScript("id")
            .execute(new AssertingActionListener<>(DeleteStoredScriptAction.NAME, client.threadPool()));
        client.prepareIndex("idx")
            .setId("id")
            .setSource("source", MediaTypeRegistry.JSON)
            .execute(new AssertingActionListener<>(IndexAction.NAME, client.threadPool()));

        // choosing arbitrary cluster admin actions to test
        client.admin().cluster().prepareClusterStats().execute(new AssertingActionListener<>(ClusterStatsAction.NAME, client.threadPool()));
        client.admin()
            .cluster()
            .prepareCreateSnapshot("repo", "bck")
            .execute(new AssertingActionListener<>(CreateSnapshotAction.NAME, client.threadPool()));
        client.admin().cluster().prepareReroute().execute(new AssertingActionListener<>(ClusterRerouteAction.NAME, client.threadPool()));

        // choosing arbitrary indices admin actions to test
        client.admin().indices().prepareCreate("idx").execute(new AssertingActionListener<>(CreateIndexAction.NAME, client.threadPool()));
        client.admin().indices().prepareStats().execute(new AssertingActionListener<>(IndicesStatsAction.NAME, client.threadPool()));
        client.admin()
            .indices()
            .prepareClearCache("idx1", "idx2")
            .execute(new AssertingActionListener<>(ClearIndicesCacheAction.NAME, client.threadPool()));
        client.admin().indices().prepareFlush().execute(new AssertingActionListener<>(FlushAction.NAME, client.threadPool()));
    }

    public void testOverrideHeader() throws Exception {
        String key1Val = randomAlphaOfLength(5);
        Map<String, String> expected = new HashMap<>();
        expected.put("key1", key1Val);
        expected.put("key2", "val 2");
        client.threadPool().getThreadContext().putHeader("key1", key1Val);
        client.prepareGet("idx", "id").execute(new AssertingActionListener<>(GetAction.NAME, expected, client.threadPool()));

        client.admin()
            .cluster()
            .prepareClusterStats()
            .execute(new AssertingActionListener<>(ClusterStatsAction.NAME, expected, client.threadPool()));

        client.admin()
            .indices()
            .prepareCreate("idx")
            .execute(new AssertingActionListener<>(CreateIndexAction.NAME, expected, client.threadPool()));
    }

    protected static void assertHeaders(Map<String, String> headers, Map<String, String> expected) {
        assertNotNull(headers);
        headers = new HashMap<>(headers);
        headers.remove("transport_client"); // default header on TPC
        assertEquals(expected.size(), headers.size());
        for (Map.Entry<String, String> expectedEntry : expected.entrySet()) {
            assertEquals(headers.get(expectedEntry.getKey()), expectedEntry.getValue());
        }
    }

    protected static void assertHeaders(ThreadPool pool) {
        Settings asSettings = HEADER_SETTINGS.getAsSettings(ThreadContext.PREFIX);
        assertHeaders(
            pool.getThreadContext().getHeaders(),
            asSettings.keySet().stream().collect(Collectors.toMap(Function.identity(), k -> asSettings.get(k)))
        );
    }

    public static class InternalException extends Exception {

        private final String action;

        public InternalException(String action) {
            this.action = action;
        }
    }

    protected static class AssertingActionListener<T> implements ActionListener<T> {

        private final String action;
        private final Map<String, String> expectedHeaders;
        private final ThreadPool pool;
        private static final Settings THREAD_HEADER_SETTINGS = HEADER_SETTINGS.getAsSettings(ThreadContext.PREFIX);

        public AssertingActionListener(String action, ThreadPool pool) {
            this(
                action,
                THREAD_HEADER_SETTINGS.keySet().stream().collect(Collectors.toMap(Function.identity(), k -> THREAD_HEADER_SETTINGS.get(k))),
                pool
            );
        }

        public AssertingActionListener(String action, Map<String, String> expectedHeaders, ThreadPool pool) {
            this.action = action;
            this.expectedHeaders = expectedHeaders;
            this.pool = pool;
        }

        @Override
        public void onResponse(T t) {
            fail("an internal exception was expected for action [" + action + "]");
        }

        @Override
        public void onFailure(Exception t) {
            Throwable e = unwrap(t, InternalException.class);
            assertThat("expected action [" + action + "] to throw an internal exception", e, notNullValue());
            assertThat(action, equalTo(((InternalException) e).action));
            Map<String, String> headers = pool.getThreadContext().getHeaders();
            assertHeaders(headers, expectedHeaders);
        }

        public Throwable unwrap(Throwable t, Class<? extends Throwable> exceptionType) {
            int counter = 0;
            Throwable result = t;
            while (!exceptionType.isInstance(result)) {
                if (result.getCause() == null) {
                    return null;
                }
                if (result.getCause() == result) {
                    return null;
                }
                if (counter++ > 10) {
                    // dear god, if we got more than 10 levels down, WTF? just bail
                    fail("Exception cause unwrapping ran for 10 levels: " + ExceptionsHelper.stackTrace(t));
                    return null;
                }
                result = result.getCause();
            }
            return result;
        }
    }
}
