/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilders;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

@AwaitsFix(bugUrl = "DSL query pipeline not fully wired E2E: fragment conversion, shard execution and Arrow Flight drain pending")
public class DslRangeQueryIT extends DslIntegTestBase {

    public void testRangeQueryWithNumericBounds() {
        createIndex("products");
        client().prepareIndex("products").setId("1").setSource("{\"price\": 100}", XContentType.JSON).get();
        client().prepareIndex("products").setId("2").setSource("{\"price\": 200}", XContentType.JSON).get();
        client().prepareIndex("products").setId("3").setSource("{\"price\": 300}", XContentType.JSON).get();
        refresh("products");

        SearchResponse response = client().prepareSearch("products").setQuery(QueryBuilders.rangeQuery("price").gte(150).lte(250)).get();

        assertEquals(1, response.getHits().getTotalHits().value());
    }

    public void testRangeQueryWithDateFormat() {
        createIndex("events");
        client().prepareIndex("events").setId("1").setSource("{\"created\": \"2022-01-15T10:00:00Z\"}", XContentType.JSON).get();
        client().prepareIndex("events").setId("2").setSource("{\"created\": \"2022-02-15T10:00:00Z\"}", XContentType.JSON).get();
        client().prepareIndex("events").setId("3").setSource("{\"created\": \"2022-03-15T10:00:00Z\"}", XContentType.JSON).get();
        refresh("events");

        SearchResponse response = client().prepareSearch("events")
            .setQuery(QueryBuilders.rangeQuery("created").gte("01/02/2022").lte("28/02/2022").format("dd/MM/yyyy"))
            .get();

        assertEquals(1, response.getHits().getTotalHits().value());
    }

    public void testRangeQueryWithAutoRoundUp() {
        createIndex("logs");
        client().prepareIndex("logs").setId("1").setSource("{\"timestamp\": \"2024-03-31T10:00:00Z\"}", XContentType.JSON).get();
        client().prepareIndex("logs").setId("2").setSource("{\"timestamp\": \"2024-03-31T23:59:59.999Z\"}", XContentType.JSON).get();
        client().prepareIndex("logs").setId("3").setSource("{\"timestamp\": \"2024-04-01T00:00:00Z\"}", XContentType.JSON).get();
        refresh("logs");

        SearchResponse response = client().prepareSearch("logs").setQuery(QueryBuilders.rangeQuery("timestamp").lte("2024-03-31")).get();

        assertEquals(2, response.getHits().getTotalHits().value());
    }

    public void testRangeQueryWithExplicitRounding() {
        createIndex("logs");
        long now = System.currentTimeMillis();
        ZonedDateTime nowZdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.of("UTC"));
        ZonedDateTime startOfToday = nowZdt.toLocalDate().atStartOfDay(ZoneId.of("UTC"));

        client().prepareIndex("logs")
            .setId("1")
            .setSource("{\"timestamp\": " + (startOfToday.toInstant().toEpochMilli() - 86400000) + "}", XContentType.JSON)
            .get();
        client().prepareIndex("logs")
            .setId("2")
            .setSource("{\"timestamp\": " + startOfToday.toInstant().toEpochMilli() + "}", XContentType.JSON)
            .get();
        client().prepareIndex("logs").setId("3").setSource("{\"timestamp\": " + now + "}", XContentType.JSON).get();
        refresh("logs");

        SearchResponse response = client().prepareSearch("logs").setQuery(QueryBuilders.rangeQuery("timestamp").gte("now/d")).get();

        assertTrue(response.getHits().getTotalHits().value() >= 2);
    }

    public void testRangeQueryWithTimezone() {
        createIndex("events");
        client().prepareIndex("events").setId("1").setSource("{\"created\": \"2022-01-01T05:00:00Z\"}", XContentType.JSON).get();
        client().prepareIndex("events").setId("2").setSource("{\"created\": \"2022-01-01T10:00:00Z\"}", XContentType.JSON).get();
        refresh("events");

        SearchResponse response = client().prepareSearch("events")
            .setQuery(QueryBuilders.rangeQuery("created").gte("2022-01-01").timeZone("America/New_York"))
            .get();

        assertEquals(2, response.getHits().getTotalHits().value());
    }

    public void testRangeQueryBothBounds() {
        createIndex("products");
        client().prepareIndex("products").setId("1").setSource("{\"price\": 50}", XContentType.JSON).get();
        client().prepareIndex("products").setId("2").setSource("{\"price\": 150}", XContentType.JSON).get();
        client().prepareIndex("products").setId("3").setSource("{\"price\": 250}", XContentType.JSON).get();
        client().prepareIndex("products").setId("4").setSource("{\"price\": 350}", XContentType.JSON).get();
        refresh("products");

        SearchResponse response = client().prepareSearch("products").setQuery(QueryBuilders.rangeQuery("price").gte(100).lte(300)).get();

        assertEquals(2, response.getHits().getTotalHits().value());
    }

    public void testRangeQueryGtAndLt() {
        createIndex("products");
        client().prepareIndex("products").setId("1").setSource("{\"price\": 100}", XContentType.JSON).get();
        client().prepareIndex("products").setId("2").setSource("{\"price\": 150}", XContentType.JSON).get();
        client().prepareIndex("products").setId("3").setSource("{\"price\": 200}", XContentType.JSON).get();
        refresh("products");

        SearchResponse response = client().prepareSearch("products").setQuery(QueryBuilders.rangeQuery("price").gt(100).lt(200)).get();

        assertEquals(1, response.getHits().getTotalHits().value());
    }

    public void testRangeQueryWithRelation() {
        createIndex("events");
        client().prepareIndex("events").setId("1").setSource("{\"created\": \"2022-01-15T10:00:00Z\"}", XContentType.JSON).get();
        client().prepareIndex("events").setId("2").setSource("{\"created\": \"2022-02-15T10:00:00Z\"}", XContentType.JSON).get();
        refresh("events");

        SearchResponse response = client().prepareSearch("events")
            .setQuery(QueryBuilders.rangeQuery("created").gte("2022-01-01").lte("2022-01-31").relation("INTERSECTS"))
            .get();

        assertEquals(1, response.getHits().getTotalHits().value());
    }
}
