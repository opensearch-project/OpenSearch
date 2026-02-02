/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.MockSearchService;
import org.opensearch.search.SearchService;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

public class StackDumperTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(MockSearchService.TestPlugin.class, MockTransportService.TestPlugin.class);
    }

    public void testStackDump() throws IOException, ExecutionException, InterruptedException {
        TransportService transportService = getInstanceFromNode(TransportService.class);
        StackDumper stackDumper = transportService.getTaskManager().getStackDumper();
        stackDumper.setInterval(TimeValue.timeValueMillis(100));
        stackDumper.setTaskStackDumpThresholdNano(TimeValue.timeValueNanos(1));
        stackDumper.setTaskDumpEnabled(true);

        MockSearchService searchService = (MockSearchService) getInstanceFromNode(SearchService.class);
        searchService.setOnRemoveContext(id -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        });

        createIndex("index");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.termQuery("name", 1))
            .aggregation(AggregationBuilders.terms("name1").field("name").size(100));
        SearchRequest searchRequest = new SearchRequest().indices("index").allowPartialSearchResults(true).source(searchSourceBuilder);

        client().search(searchRequest).get();

        Path path = stackDumper.getPathForSlowTaskDump();
        assertNotNull(path);
        assertTrue(Files.exists(path.resolve("0.stack")));
        assertFalse(Files.exists(path.resolve("1.stack")));
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
            for (Path file : stream) {
                Files.delete(file);
            }
        }
        IOUtils.rm(path);
    }
}