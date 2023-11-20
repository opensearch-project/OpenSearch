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

package org.opensearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.lucene.search.TotalHits;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.logging.MockAppender;
import org.opensearch.common.logging.SlowLogLevel;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SearchRequestSlowLogTests extends OpenSearchTestCase {
    static MockAppender appender;
    static Logger logger = LogManager.getLogger(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_PREFIX + ".SearchRequestSlowLog");

    @BeforeClass
    public static void init() throws IllegalAccessException {
        appender = new MockAppender("trace_appender");
        appender.start();
        Loggers.addAppender(logger, appender);
    }

    @AfterClass
    public static void cleanup() {
        Loggers.removeAppender(logger, appender);
        appender.stop();
    }

    public void testMultipleSlowLoggersUseSingleLog4jLogger() {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);

        SearchPhaseContext searchPhaseContext1 = new MockSearchPhaseContext(1);
        ClusterService clusterService1 = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null
        );
        SearchRequestSlowLog searchRequestSlowLog1 = new SearchRequestSlowLog(clusterService1);
        int numberOfLoggersBefore = context.getLoggers().size();

        SearchPhaseContext searchPhaseContext2 = new MockSearchPhaseContext(1);
        ClusterService clusterService2 = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null
        );
        SearchRequestSlowLog searchRequestSlowLog2 = new SearchRequestSlowLog(clusterService2);

        int numberOfLoggersAfter = context.getLoggers().size();
        assertThat(numberOfLoggersAfter, equalTo(numberOfLoggersBefore));
    }

    public void testOnRequestEnd() throws InterruptedException {
        final Logger logger = mock(Logger.class);
        final SearchRequestContext searchRequestContext = mock(SearchRequestContext.class);
        final SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        final SearchRequest searchRequest = mock(SearchRequest.class);
        final SearchTask searchTask = mock(SearchTask.class);

        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_INFO_SETTING.getKey(), "0ms");
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_DEBUG_SETTING.getKey(), "0ms");
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_TRACE_SETTING.getKey(), "0ms");
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = new ClusterService(settings, clusterSettings, null);
        SearchRequestSlowLog searchRequestSlowLog = new SearchRequestSlowLog(clusterService, logger);
        final List<SearchRequestOperationsListener> searchListenersList = new ArrayList<>(List.of(searchRequestSlowLog));

        when(searchRequestContext.getSearchRequestOperationsListener()).thenReturn(
            new SearchRequestOperationsListener.CompositeListener(searchListenersList, logger)
        );
        when(searchRequestContext.getAbsoluteStartNanos()).thenReturn(System.nanoTime() - 1L);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        when(searchPhaseContext.getTask()).thenReturn(searchTask);
        when(searchRequest.searchType()).thenReturn(SearchType.QUERY_THEN_FETCH);

        searchRequestContext.getSearchRequestOperationsListener().onRequestEnd(searchPhaseContext, searchRequestContext);

        verify(logger, never()).warn(any(SearchRequestSlowLog.SearchRequestSlowLogMessage.class));
        verify(logger, times(1)).info(any(SearchRequestSlowLog.SearchRequestSlowLogMessage.class));
        verify(logger, never()).debug(any(SearchRequestSlowLog.SearchRequestSlowLogMessage.class));
        verify(logger, never()).trace(any(SearchRequestSlowLog.SearchRequestSlowLogMessage.class));
    }

    public void testConcurrentOnRequestEnd() throws InterruptedException {
        final Logger logger = mock(Logger.class);
        final SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        final SearchRequest searchRequest = mock(SearchRequest.class);
        final SearchTask searchTask = mock(SearchTask.class);

        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_WARN_SETTING.getKey(), "-1");
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_INFO_SETTING.getKey(), "10s");
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_DEBUG_SETTING.getKey(), "-1");
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_TRACE_SETTING.getKey(), "-1");
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = new ClusterService(settings, clusterSettings, null);
        SearchRequestSlowLog searchRequestSlowLog = new SearchRequestSlowLog(clusterService, logger);
        final List<SearchRequestOperationsListener> searchListenersList = new ArrayList<>(List.of(searchRequestSlowLog));

        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        when(searchPhaseContext.getTask()).thenReturn(searchTask);
        when(searchRequest.searchType()).thenReturn(SearchType.QUERY_THEN_FETCH);

        int numRequests = 50;
        int numRequestsLogged = randomIntBetween(0, 50);
        Thread[] threads = new Thread[numRequests];
        Phaser phaser = new Phaser(numRequests + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numRequests);

        // create a list of SearchRequestContexts
        // each SearchRequestContext contains unique composite SearchRequestOperationsListener
        ArrayList<SearchRequestContext> searchRequestContexts = new ArrayList<>();
        for (int i = 0; i < numRequests; i++) {
            SearchRequestContext searchRequestContext = new SearchRequestContext(
                new SearchRequestOperationsListener.CompositeListener(searchListenersList, logger)
            );
            searchRequestContext.setAbsoluteStartNanos((i < numRequestsLogged) ? 0 : System.nanoTime());
            searchRequestContexts.add(searchRequestContext);
        }

        for (int i = 0; i < numRequests; i++) {
            int finalI = i;
            threads[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                SearchRequestContext thisContext = searchRequestContexts.get(finalI);
                thisContext.getSearchRequestOperationsListener().onRequestEnd(searchPhaseContext, thisContext);
                countDownLatch.countDown();
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();

        verify(logger, never()).warn(any(SearchRequestSlowLog.SearchRequestSlowLogMessage.class));
        verify(logger, times(numRequestsLogged)).info(any(SearchRequestSlowLog.SearchRequestSlowLogMessage.class));
        verify(logger, never()).debug(any(SearchRequestSlowLog.SearchRequestSlowLogMessage.class));
        verify(logger, never()).trace(any(SearchRequestSlowLog.SearchRequestSlowLogMessage.class));
    }

    public void testSearchRequestSlowLogHasJsonFields_EmptySearchRequestContext() throws IOException {
        SearchSourceBuilder source = SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery());
        SearchRequest searchRequest = new SearchRequest().source(source);
        SearchPhaseContext searchPhaseContext = new MockSearchPhaseContext(1, searchRequest);
        SearchRequestContext searchRequestContext = new SearchRequestContext();
        SearchRequestSlowLog.SearchRequestSlowLogMessage p = new SearchRequestSlowLog.SearchRequestSlowLogMessage(
            searchPhaseContext,
            10,
            searchRequestContext
        );

        assertThat(p.getValueFor("took"), equalTo("10nanos"));
        assertThat(p.getValueFor("took_millis"), equalTo("0"));
        assertThat(p.getValueFor("phase_took"), equalTo("{}"));
        assertThat(p.getValueFor("total_hits"), equalTo("-1"));
        assertThat(p.getValueFor("search_type"), equalTo("QUERY_THEN_FETCH"));
        assertThat(p.getValueFor("shards"), equalTo(""));
        assertThat(p.getValueFor("source"), equalTo("{\\\"query\\\":{\\\"match_all\\\":{\\\"boost\\\":1.0}}}"));
        assertThat(p.getValueFor("id"), equalTo(null));
    }

    public void testSearchRequestSlowLogHasJsonFields_NotEmptySearchRequestContext() throws IOException {
        SearchSourceBuilder source = SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery());
        SearchRequest searchRequest = new SearchRequest().source(source);
        SearchPhaseContext searchPhaseContext = new MockSearchPhaseContext(1, searchRequest);
        SearchRequestContext searchRequestContext = new SearchRequestContext();
        searchRequestContext.updatePhaseTookMap(SearchPhaseName.FETCH.getName(), 10L);
        searchRequestContext.updatePhaseTookMap(SearchPhaseName.QUERY.getName(), 50L);
        searchRequestContext.updatePhaseTookMap(SearchPhaseName.EXPAND.getName(), 5L);
        searchRequestContext.setTotalHits(new TotalHits(3L, TotalHits.Relation.EQUAL_TO));
        searchRequestContext.setShardStats(10, 8, 1, 1);
        SearchRequestSlowLog.SearchRequestSlowLogMessage p = new SearchRequestSlowLog.SearchRequestSlowLogMessage(
            searchPhaseContext,
            10,
            searchRequestContext
        );

        assertThat(p.getValueFor("took"), equalTo("10nanos"));
        assertThat(p.getValueFor("took_millis"), equalTo("0"));
        assertThat(p.getValueFor("phase_took"), equalTo("{expand=5, fetch=10, query=50}"));
        assertThat(p.getValueFor("total_hits"), equalTo("3 hits"));
        assertThat(p.getValueFor("search_type"), equalTo("QUERY_THEN_FETCH"));
        assertThat(p.getValueFor("shards"), equalTo("{total:10, successful:8, skipped:1, failed:1}"));
        assertThat(p.getValueFor("source"), equalTo("{\\\"query\\\":{\\\"match_all\\\":{\\\"boost\\\":1.0}}}"));
        assertThat(p.getValueFor("id"), equalTo(null));
    }

    public void testSearchRequestSlowLogHasJsonFields_PartialContext() throws IOException {
        SearchSourceBuilder source = SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery());
        SearchRequest searchRequest = new SearchRequest().source(source);
        SearchPhaseContext searchPhaseContext = new MockSearchPhaseContext(1, searchRequest);
        SearchRequestContext searchRequestContext = new SearchRequestContext();
        searchRequestContext.updatePhaseTookMap(SearchPhaseName.FETCH.getName(), 10L);
        searchRequestContext.updatePhaseTookMap(SearchPhaseName.QUERY.getName(), 50L);
        searchRequestContext.updatePhaseTookMap(SearchPhaseName.EXPAND.getName(), 5L);
        searchRequestContext.setTotalHits(new TotalHits(3L, TotalHits.Relation.EQUAL_TO));
        searchRequestContext.setShardStats(5, 3, 1, 1);
        SearchRequestSlowLog.SearchRequestSlowLogMessage p = new SearchRequestSlowLog.SearchRequestSlowLogMessage(
            searchPhaseContext,
            10000000000L,
            searchRequestContext
        );

        assertThat(p.getValueFor("took"), equalTo("10s"));
        assertThat(p.getValueFor("took_millis"), equalTo("10000"));
        assertThat(p.getValueFor("phase_took"), equalTo("{expand=5, fetch=10, query=50}"));
        assertThat(p.getValueFor("total_hits"), equalTo("3 hits"));
        assertThat(p.getValueFor("search_type"), equalTo("QUERY_THEN_FETCH"));
        assertThat(p.getValueFor("shards"), equalTo("{total:5, successful:3, skipped:1, failed:1}"));
        assertThat(p.getValueFor("source"), equalTo("{\\\"query\\\":{\\\"match_all\\\":{\\\"boost\\\":1.0}}}"));
        assertThat(p.getValueFor("id"), equalTo(null));
    }

    public void testSearchRequestSlowLogSearchContextPrinterToLog() throws IOException {
        SearchSourceBuilder source = SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery());
        SearchRequest searchRequest = new SearchRequest().source(source);
        SearchPhaseContext searchPhaseContext = new MockSearchPhaseContext(1, searchRequest);
        SearchRequestContext searchRequestContext = new SearchRequestContext();
        searchRequestContext.updatePhaseTookMap(SearchPhaseName.FETCH.getName(), 10L);
        searchRequestContext.updatePhaseTookMap(SearchPhaseName.QUERY.getName(), 50L);
        searchRequestContext.updatePhaseTookMap(SearchPhaseName.EXPAND.getName(), 5L);
        searchRequestContext.setTotalHits(new TotalHits(3L, TotalHits.Relation.EQUAL_TO));
        searchRequestContext.setShardStats(10, 8, 1, 1);
        SearchRequestSlowLog.SearchRequestSlowLogMessage p = new SearchRequestSlowLog.SearchRequestSlowLogMessage(
            searchPhaseContext,
            100000,
            searchRequestContext
        );

        assertThat(p.getFormattedMessage(), startsWith("took[100micros]"));
        assertThat(p.getFormattedMessage(), containsString("took_millis[0]"));
        assertThat(p.getFormattedMessage(), containsString("phase_took_millis[{expand=5, fetch=10, query=50}]"));
        assertThat(p.getFormattedMessage(), containsString("total_hits[3 hits]"));
        assertThat(p.getFormattedMessage(), containsString("search_type[QUERY_THEN_FETCH]"));
        assertThat(p.getFormattedMessage(), containsString("shards[{total:10, successful:8, skipped:1, failed:1}]"));
        assertThat(p.getFormattedMessage(), containsString("source[{\"query\":{\"match_all\":{\"boost\":1.0}}}]"));
        // Makes sure that output doesn't contain any new lines
        assertThat(p.getFormattedMessage(), not(containsString("\n")));
        assertThat(p.getFormattedMessage(), endsWith("id[]"));
    }

    public void testLevelSettingWarn() {
        SlowLogLevel level = SlowLogLevel.WARN;
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_LEVEL.getKey(), level);
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = new ClusterService(settings, clusterSettings, null);
        SearchRequestSlowLog searchRequestSlowLog = new SearchRequestSlowLog(clusterService);
        assertEquals(level, searchRequestSlowLog.getLevel());
    }

    public void testLevelSettingDebug() {
        String level = "DEBUG";
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_LEVEL.getKey(), level);
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = new ClusterService(settings, clusterSettings, null);
        SearchRequestSlowLog searchRequestSlowLog = new SearchRequestSlowLog(clusterService);
        assertEquals(level, searchRequestSlowLog.getLevel().toString());
    }

    public void testLevelSettingFail() {
        String level = "NOT A LEVEL";
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_LEVEL.getKey(), level);
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = new ClusterService(settings, clusterSettings, null);

        try {
            new SearchRequestSlowLog(clusterService);
            fail();
        } catch (IllegalArgumentException ex) {
            final String expected = "No enum constant org.opensearch.common.logging.SlowLogLevel.NOT A LEVEL";
            assertThat(ex, hasToString(containsString(expected)));
            assertThat(ex, instanceOf(IllegalArgumentException.class));
        }
    }

    public void testSetThresholds() {
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_WARN_SETTING.getKey(), "400ms");
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_INFO_SETTING.getKey(), "300ms");
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_DEBUG_SETTING.getKey(), "200ms");
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_TRACE_SETTING.getKey(), "100ms");
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = new ClusterService(settings, clusterSettings, null);
        SearchRequestSlowLog searchRequestSlowLog = new SearchRequestSlowLog(clusterService);
        assertEquals(TimeValue.timeValueMillis(400).nanos(), searchRequestSlowLog.getWarnThreshold());
        assertEquals(TimeValue.timeValueMillis(300).nanos(), searchRequestSlowLog.getInfoThreshold());
        assertEquals(TimeValue.timeValueMillis(200).nanos(), searchRequestSlowLog.getDebugThreshold());
        assertEquals(TimeValue.timeValueMillis(100).nanos(), searchRequestSlowLog.getTraceThreshold());
        assertEquals(SlowLogLevel.TRACE, searchRequestSlowLog.getLevel());
    }

    public void testSetThresholdsUnits() {
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_WARN_SETTING.getKey(), "400s");
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_INFO_SETTING.getKey(), "300ms");
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_DEBUG_SETTING.getKey(), "200micros");
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_TRACE_SETTING.getKey(), "100nanos");
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = new ClusterService(settings, clusterSettings, null);
        SearchRequestSlowLog searchRequestSlowLog = new SearchRequestSlowLog(clusterService);
        assertEquals(TimeValue.timeValueSeconds(400).nanos(), searchRequestSlowLog.getWarnThreshold());
        assertEquals(TimeValue.timeValueMillis(300).nanos(), searchRequestSlowLog.getInfoThreshold());
        assertEquals(TimeValue.timeValueNanos(200000).nanos(), searchRequestSlowLog.getDebugThreshold());
        assertEquals(TimeValue.timeValueNanos(100).nanos(), searchRequestSlowLog.getTraceThreshold());
        assertEquals(SlowLogLevel.TRACE, searchRequestSlowLog.getLevel());
    }

    public void testSetThresholdsDefaults() {
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_WARN_SETTING.getKey(), "400ms");
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_DEBUG_SETTING.getKey(), "200ms");
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = new ClusterService(settings, clusterSettings, null);
        SearchRequestSlowLog searchRequestSlowLog = new SearchRequestSlowLog(clusterService);
        assertEquals(TimeValue.timeValueMillis(400).nanos(), searchRequestSlowLog.getWarnThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), searchRequestSlowLog.getInfoThreshold());
        assertEquals(TimeValue.timeValueMillis(200).nanos(), searchRequestSlowLog.getDebugThreshold());
        assertEquals(TimeValue.timeValueMillis(-1).nanos(), searchRequestSlowLog.getTraceThreshold());
        assertEquals(SlowLogLevel.TRACE, searchRequestSlowLog.getLevel());
    }

    public void testSetThresholdsError() {
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_WARN_SETTING.getKey(), "NOT A TIME VALUE");
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = new ClusterService(settings, clusterSettings, null);

        try {
            new SearchRequestSlowLog(clusterService);
            fail();
        } catch (IllegalArgumentException ex) {
            final String expected =
                "failed to parse setting [cluster.search.request.slowlog.threshold.warn] with value [NOT A TIME VALUE] as a time value";
            assertThat(ex, hasToString(containsString(expected)));
            assertThat(ex, instanceOf(IllegalArgumentException.class));
        }
    }
}
