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

package org.opensearch.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.opensearch.Version;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.logging.MockAppender;
import org.opensearch.common.logging.SlowLogLevel;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.test.TestSearchContext;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class SearchSlowLogTests extends OpenSearchSingleNodeTestCase {
    static MockAppender appender;
    static Logger queryLog = LogManager.getLogger(SearchSlowLog.INDEX_SEARCH_SLOWLOG_PREFIX + ".query");
    static Logger fetchLog = LogManager.getLogger(SearchSlowLog.INDEX_SEARCH_SLOWLOG_PREFIX + ".fetch");

    @BeforeClass
    public static void init() throws IllegalAccessException {
        appender = new MockAppender("trace_appender");
        appender.start();
        Loggers.addAppender(queryLog, appender);
        Loggers.addAppender(fetchLog, appender);
    }

    @AfterClass
    public static void cleanup() {
        Loggers.removeAppender(queryLog, appender);
        Loggers.removeAppender(fetchLog, appender);
        appender.stop();
    }

    @Override
    protected SearchContext createSearchContext(IndexService indexService) {
        return createSearchContext(indexService, new String[] {});
    }

    protected SearchContext createSearchContext(IndexService indexService, String... groupStats) {
        BigArrays bigArrays = indexService.getBigArrays();
        final ShardSearchRequest request = new ShardSearchRequest(new ShardId(indexService.index(), 0), 0L, null);
        return new TestSearchContext(bigArrays, indexService) {
            @Override
            public List<String> groupStats() {
                return Arrays.asList(groupStats);
            }

            @Override
            public ShardSearchRequest request() {
                return request;
            }

            @Override
            public SearchShardTask getTask() {
                return super.getTask();
            }
        };
    }

    public void testLevelPrecedence() {
        SearchContext ctx = searchContextWithSourceAndTask(createIndex("index"));
        String uuid = UUIDs.randomBase64UUID();
        IndexSettings settings = new IndexSettings(createIndexMetadata(SlowLogLevel.WARN, "index", uuid), Settings.EMPTY);
        SearchSlowLog log = new SearchSlowLog(settings);

        {
            // level set to WARN, should only log when WARN limit is breached
            log.onQueryPhase(ctx, 40L);
            assertNull(appender.getLastEventAndReset());
            log.onQueryPhase(ctx, 41L);
            assertNotNull(appender.getLastEventAndReset());

            log.onFetchPhase(ctx, 40L);
            assertNull(appender.getLastEventAndReset());
            log.onFetchPhase(ctx, 41L);
            assertNotNull(appender.getLastEventAndReset());
        }

        {
            // level set INFO, should log when INFO level is breached
            settings.updateIndexMetadata(createIndexMetadata(SlowLogLevel.INFO, "index", uuid));
            log.onQueryPhase(ctx, 30L);
            assertNull(appender.getLastEventAndReset());
            log.onQueryPhase(ctx, 31L);
            assertNotNull(appender.getLastEventAndReset());

            log.onFetchPhase(ctx, 30L);
            assertNull(appender.getLastEventAndReset());
            log.onFetchPhase(ctx, 31L);
            assertNotNull(appender.getLastEventAndReset());
        }

        {
            // level set DEBUG, should log when DEBUG level is breached
            settings.updateIndexMetadata(createIndexMetadata(SlowLogLevel.DEBUG, "index", uuid));
            log.onQueryPhase(ctx, 20L);
            assertNull(appender.getLastEventAndReset());
            log.onQueryPhase(ctx, 21L);
            assertNotNull(appender.getLastEventAndReset());

            log.onFetchPhase(ctx, 20L);
            assertNull(appender.getLastEventAndReset());
            log.onFetchPhase(ctx, 21L);
            assertNotNull(appender.getLastEventAndReset());
        }

        {
            // level set TRACE, should log when TRACE level is breached
            settings.updateIndexMetadata(createIndexMetadata(SlowLogLevel.TRACE, "index", uuid));
            log.onQueryPhase(ctx, 10L);
            assertNull(appender.getLastEventAndReset());
            log.onQueryPhase(ctx, 11L);
            assertNotNull(appender.getLastEventAndReset());

            log.onFetchPhase(ctx, 10L);
            assertNull(appender.getLastEventAndReset());
            log.onFetchPhase(ctx, 11L);
            assertNotNull(appender.getLastEventAndReset());
        }
    }

    public void testTwoLoggersDifferentLevel() {
        SearchContext ctx1 = searchContextWithSourceAndTask(createIndex("index-1"));
        SearchContext ctx2 = searchContextWithSourceAndTask(createIndex("index-2"));
        IndexSettings settings1 = new IndexSettings(
            createIndexMetadata(SlowLogLevel.WARN, "index-1", UUIDs.randomBase64UUID()),
            Settings.EMPTY
        );
        SearchSlowLog log1 = new SearchSlowLog(settings1);

        IndexSettings settings2 = new IndexSettings(
            createIndexMetadata(SlowLogLevel.TRACE, "index-2", UUIDs.randomBase64UUID()),
            Settings.EMPTY
        );
        SearchSlowLog log2 = new SearchSlowLog(settings2);

        {
            // level set WARN, should not log
            log1.onQueryPhase(ctx1, 11L);
            assertNull(appender.getLastEventAndReset());
            log1.onFetchPhase(ctx1, 11L);
            assertNull(appender.getLastEventAndReset());

            // level set TRACE, should log
            log2.onQueryPhase(ctx2, 11L);
            assertNotNull(appender.getLastEventAndReset());
            log2.onFetchPhase(ctx2, 11L);
            assertNotNull(appender.getLastEventAndReset());
        }
    }

    public void testMultipleSlowLoggersUseSingleLog4jLogger() {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);

        IndexService index1 = createIndex("index-1");
        IndexService index2 = createIndex("index-2");

        SearchContext ctx1 = searchContextWithSourceAndTask(index1);
        IndexSettings settings1 = new IndexSettings(
            createIndexMetadata(SlowLogLevel.WARN, "index-1", UUIDs.randomBase64UUID()),
            Settings.EMPTY
        );
        SearchSlowLog log1 = new SearchSlowLog(settings1);
        int numberOfLoggersBefore = context.getLoggers().size();

        SearchContext ctx2 = searchContextWithSourceAndTask(index2);
        IndexSettings settings2 = new IndexSettings(
            createIndexMetadata(SlowLogLevel.TRACE, "index-2", UUIDs.randomBase64UUID()),
            Settings.EMPTY
        );
        SearchSlowLog log2 = new SearchSlowLog(settings2);

        int numberOfLoggersAfter = context.getLoggers().size();
        assertThat(numberOfLoggersAfter, equalTo(numberOfLoggersBefore));
    }

    private IndexMetadata createIndexMetadata(SlowLogLevel level, String index, String uuid) {
        return newIndexMeta(
            index,
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, uuid)
                .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_LEVEL.getKey(), level)
                .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING.getKey(), "10nanos")
                .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING.getKey(), "20nanos")
                .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING.getKey(), "30nanos")
                .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING.getKey(), "40nanos")

                .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING.getKey(), "10nanos")
                .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING.getKey(), "20nanos")
                .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING.getKey(), "30nanos")
                .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING.getKey(), "40nanos")
                .build()
        );
    }

    public void testSlowLogHasJsonFields() throws IOException {
        IndexService index = createIndex("foo");
        SearchContext searchContext = searchContextWithSourceAndTask(index);
        SearchSlowLog.SearchSlowLogMessage p = new SearchSlowLog.SearchSlowLogMessage(searchContext, 10);

        assertThat(p.getValueFor("message"), equalTo("[foo][0]"));
        assertThat(p.getValueFor("took"), equalTo("10nanos"));
        assertThat(p.getValueFor("took_millis"), equalTo("0"));
        assertThat(p.getValueFor("total_hits"), equalTo("-1"));
        assertThat(p.getValueFor("stats"), equalTo("[]"));
        assertThat(p.getValueFor("search_type"), Matchers.nullValue());
        assertThat(p.getValueFor("total_shards"), equalTo("1"));
        assertThat(p.getValueFor("source"), equalTo("{\\\"query\\\":{\\\"match_all\\\":{\\\"boost\\\":1.0}}}"));
    }

    public void testSlowLogsWithStats() throws IOException {
        IndexService index = createIndex("foo");
        SearchContext searchContext = createSearchContext(index, "group1");
        SearchSourceBuilder source = SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery());
        searchContext.request().source(source);
        searchContext.setTask(new SearchShardTask(0, "n/a", "n/a", "test", null, Collections.singletonMap(Task.X_OPAQUE_ID, "my_id")));

        SearchSlowLog.SearchSlowLogMessage p = new SearchSlowLog.SearchSlowLogMessage(searchContext, 10);
        assertThat(p.getValueFor("stats"), equalTo("[\\\"group1\\\"]"));

        searchContext = createSearchContext(index, "group1", "group2");
        source = SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery());
        searchContext.request().source(source);
        searchContext.setTask(new SearchShardTask(0, "n/a", "n/a", "test", null, Collections.singletonMap(Task.X_OPAQUE_ID, "my_id")));
        p = new SearchSlowLog.SearchSlowLogMessage(searchContext, 10);
        assertThat(p.getValueFor("stats"), equalTo("[\\\"group1\\\", \\\"group2\\\"]"));
    }

    public void testSlowLogSearchContextPrinterToLog() throws IOException {
        IndexService index = createIndex("foo");
        SearchContext searchContext = searchContextWithSourceAndTask(index);
        SearchSlowLog.SearchSlowLogMessage p = new SearchSlowLog.SearchSlowLogMessage(searchContext, 10);
        assertThat(p.getFormattedMessage(), startsWith("[foo][0]"));
        // Makes sure that output doesn't contain any new lines
        assertThat(p.getFormattedMessage(), not(containsString("\n")));
        assertThat(p.getFormattedMessage(), endsWith("id[my_id], "));
    }

    public void testLevelSetting() {
        SlowLogLevel level = randomFrom(SlowLogLevel.values());
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_LEVEL.getKey(), level)
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        SearchSlowLog log = new SearchSlowLog(settings);
        assertEquals(level, log.getLevel());
        level = randomFrom(SlowLogLevel.values());
        settings.updateIndexMetadata(
            newIndexMeta("index", Settings.builder().put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_LEVEL.getKey(), level).build())
        );
        assertEquals(level, log.getLevel());
        level = randomFrom(SlowLogLevel.values());
        settings.updateIndexMetadata(
            newIndexMeta("index", Settings.builder().put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_LEVEL.getKey(), level).build())
        );
        assertEquals(level, log.getLevel());

        settings.updateIndexMetadata(
            newIndexMeta("index", Settings.builder().put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_LEVEL.getKey(), level).build())
        );
        assertEquals(level, log.getLevel());

        settings.updateIndexMetadata(newIndexMeta("index", Settings.EMPTY));
        assertEquals(SlowLogLevel.TRACE, log.getLevel());

        metadata = newIndexMeta("index", Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build());
        settings = new IndexSettings(metadata, Settings.EMPTY);
        log = new SearchSlowLog(settings);
        try {
            settings.updateIndexMetadata(
                newIndexMeta("index", Settings.builder().put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_LEVEL.getKey(), "NOT A LEVEL").build())
            );
            fail();
        } catch (IllegalArgumentException ex) {
            final String expected = "illegal value can't update [index.search.slowlog.level] from [TRACE] to [NOT A LEVEL]";
            assertThat(ex, hasToString(containsString(expected)));
            assertNotNull(ex.getCause());
            assertThat(ex.getCause(), instanceOf(IllegalArgumentException.class));
            final IllegalArgumentException cause = (IllegalArgumentException) ex.getCause();
            assertThat(cause, hasToString(containsString("No enum constant org.opensearch.common.logging.SlowLogLevel.NOT A LEVEL")));
        }
        assertEquals(SlowLogLevel.TRACE, log.getLevel());

        metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_LEVEL.getKey(), SlowLogLevel.DEBUG)
                .build()
        );
        settings = new IndexSettings(metadata, Settings.EMPTY);
        SearchSlowLog debugLog = new SearchSlowLog(settings);

        metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_LEVEL.getKey(), SlowLogLevel.INFO)
                .build()
        );
        settings = new IndexSettings(metadata, Settings.EMPTY);
        SearchSlowLog infoLog = new SearchSlowLog(settings);

        assertEquals(SlowLogLevel.DEBUG, debugLog.getLevel());
        assertEquals(SlowLogLevel.INFO, infoLog.getLevel());
    }

    public void testSetQueryLevels() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING.getKey(), "100ms")
                .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING.getKey(), "200ms")
                .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING.getKey(), "300ms")
                .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING.getKey(), "400ms")
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        SearchSlowLog log = new SearchSlowLog(settings);
        assertEquals(TimeValue.timeValueMillis(100).nanos(), log.getQueryTraceThreshold());
        assertEquals(TimeValue.timeValueMillis(200).nanos(), log.getQueryDebugThreshold());
        assertEquals(TimeValue.timeValueMillis(300).nanos(), log.getQueryInfoThreshold());
        assertEquals(TimeValue.timeValueMillis(400).nanos(), log.getQueryWarnThreshold());

        settings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING.getKey(), "120ms")
                    .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING.getKey(), "220ms")
                    .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING.getKey(), "320ms")
                    .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING.getKey(), "420ms")
                    .build()
            )
        );

        assertEquals(TimeValue.timeValueMillis(120).nanos(), log.getQueryTraceThreshold());
        assertEquals(TimeValue.timeValueMillis(220).nanos(), log.getQueryDebugThreshold());
        assertEquals(TimeValue.timeValueMillis(320).nanos(), log.getQueryInfoThreshold());
        assertEquals(TimeValue.timeValueMillis(420).nanos(), log.getQueryWarnThreshold());

        metadata = newIndexMeta("index", Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build());
        settings.updateIndexMetadata(metadata);
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getQueryTraceThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getQueryDebugThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getQueryInfoThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getQueryWarnThreshold());

        settings = new IndexSettings(metadata, Settings.EMPTY);
        log = new SearchSlowLog(settings);

        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getQueryTraceThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getQueryDebugThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getQueryInfoThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getQueryWarnThreshold());
        try {
            settings.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder()
                        .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING.getKey(), "NOT A TIME VALUE")
                        .build()
                )
            );
            fail();
        } catch (IllegalArgumentException ex) {
            assertTimeValueException(ex, "index.search.slowlog.threshold.query.trace");
        }

        try {
            settings.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder()
                        .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING.getKey(), "NOT A TIME VALUE")
                        .build()
                )
            );
            fail();
        } catch (IllegalArgumentException ex) {
            assertTimeValueException(ex, "index.search.slowlog.threshold.query.debug");
        }

        try {
            settings.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder()
                        .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING.getKey(), "NOT A TIME VALUE")
                        .build()
                )
            );
            fail();
        } catch (IllegalArgumentException ex) {
            assertTimeValueException(ex, "index.search.slowlog.threshold.query.info");
        }

        try {
            settings.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder()
                        .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING.getKey(), "NOT A TIME VALUE")
                        .build()
                )
            );
            fail();
        } catch (IllegalArgumentException ex) {
            assertTimeValueException(ex, "index.search.slowlog.threshold.query.warn");
        }
    }

    public void testSetFetchLevels() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING.getKey(), "100ms")
                .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING.getKey(), "200ms")
                .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING.getKey(), "300ms")
                .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING.getKey(), "400ms")
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        SearchSlowLog log = new SearchSlowLog(settings);
        assertEquals(TimeValue.timeValueMillis(100).nanos(), log.getFetchTraceThreshold());
        assertEquals(TimeValue.timeValueMillis(200).nanos(), log.getFetchDebugThreshold());
        assertEquals(TimeValue.timeValueMillis(300).nanos(), log.getFetchInfoThreshold());
        assertEquals(TimeValue.timeValueMillis(400).nanos(), log.getFetchWarnThreshold());

        settings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING.getKey(), "120ms")
                    .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING.getKey(), "220ms")
                    .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING.getKey(), "320ms")
                    .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING.getKey(), "420ms")
                    .build()
            )
        );

        assertEquals(TimeValue.timeValueMillis(120).nanos(), log.getFetchTraceThreshold());
        assertEquals(TimeValue.timeValueMillis(220).nanos(), log.getFetchDebugThreshold());
        assertEquals(TimeValue.timeValueMillis(320).nanos(), log.getFetchInfoThreshold());
        assertEquals(TimeValue.timeValueMillis(420).nanos(), log.getFetchWarnThreshold());

        metadata = newIndexMeta("index", Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build());
        settings.updateIndexMetadata(metadata);
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getFetchTraceThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getFetchDebugThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getFetchInfoThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getFetchWarnThreshold());

        settings = new IndexSettings(metadata, Settings.EMPTY);
        log = new SearchSlowLog(settings);

        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getFetchTraceThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getFetchDebugThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getFetchInfoThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), log.getFetchWarnThreshold());
        try {
            settings.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder()
                        .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING.getKey(), "NOT A TIME VALUE")
                        .build()
                )
            );
            fail();
        } catch (IllegalArgumentException ex) {
            assertTimeValueException(ex, "index.search.slowlog.threshold.fetch.trace");
        }

        try {
            settings.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder()
                        .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING.getKey(), "NOT A TIME VALUE")
                        .build()
                )
            );
            fail();
        } catch (IllegalArgumentException ex) {
            assertTimeValueException(ex, "index.search.slowlog.threshold.fetch.debug");
        }

        try {
            settings.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder()
                        .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING.getKey(), "NOT A TIME VALUE")
                        .build()
                )
            );
            fail();
        } catch (IllegalArgumentException ex) {
            assertTimeValueException(ex, "index.search.slowlog.threshold.fetch.info");
        }

        try {
            settings.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder()
                        .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING.getKey(), "NOT A TIME VALUE")
                        .build()
                )
            );
            fail();
        } catch (IllegalArgumentException ex) {
            assertTimeValueException(ex, "index.search.slowlog.threshold.fetch.warn");
        }
    }

    private void assertTimeValueException(final IllegalArgumentException e, final String key) {
        final String expected = "illegal value can't update [" + key + "] from [-1] to [NOT A TIME VALUE]";
        assertThat(e, hasToString(containsString(expected)));
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        final IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
        final String causeExpected = "failed to parse setting ["
            + key
            + "] with value [NOT A TIME VALUE] as a time value: unit is missing or unrecognized";
        assertThat(cause, hasToString(containsString(causeExpected)));
    }

    private IndexMetadata newIndexMeta(String name, Settings indexSettings) {
        Settings build = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(indexSettings)
            .build();
        IndexMetadata metadata = IndexMetadata.builder(name).settings(build).build();
        return metadata;
    }

    private SearchContext searchContextWithSourceAndTask(IndexService index) {
        SearchContext ctx = createSearchContext(index);
        SearchSourceBuilder source = SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery());
        ctx.request().source(source);
        ctx.setTask(new SearchShardTask(0, "n/a", "n/a", "test", null, Collections.singletonMap(Task.X_OPAQUE_ID, "my_id")));
        return ctx;
    }
}
