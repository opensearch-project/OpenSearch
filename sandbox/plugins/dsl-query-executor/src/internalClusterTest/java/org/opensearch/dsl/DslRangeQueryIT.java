/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.QueryBuilders;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class DslRangeQueryIT extends DslIntegTestBase {

    public void testRangeQueryWithNumericBounds() {
        createIndex("products");
        indexDoc("products", "1", "{\"price\": 100}");
        indexDoc("products", "2", "{\"price\": 200}");
        indexDoc("products", "3", "{\"price\": 300}");
        refresh("products");

        SearchResponse response = client().prepareSearch("products").setQuery(QueryBuilders.rangeQuery("price").gte(150).lte(250)).get();

        assertEquals(1, response.getHits().getTotalHits().value);
    }

    public void testRangeQueryWithDateFormat() {
        createIndex("events");
        indexDoc("events", "1", "{\"created\": \"2022-01-15T10:00:00Z\"}");
        indexDoc("events", "2", "{\"created\": \"2022-02-15T10:00:00Z\"}");
        indexDoc("events", "3", "{\"created\": \"2022-03-15T10:00:00Z\"}");
        refresh("events");

        SearchResponse response = client().prepareSearch("events")
            .setQuery(QueryBuilders.rangeQuery("created").gte("01/02/2022").lte("28/02/2022").format("dd/MM/yyyy"))
            .get();

        assertEquals(1, response.getHits().getTotalHits().value);
    }

    public void testRangeQueryWithAutoRoundUp() {
        createIndex("logs");
        indexDoc("logs", "1", "{\"timestamp\": \"2024-03-31T10:00:00Z\"}");
        indexDoc("logs", "2", "{\"timestamp\": \"2024-03-31T23:59:59.999Z\"}");
        indexDoc("logs", "3", "{\"timestamp\": \"2024-04-01T00:00:00Z\"}");
        refresh("logs");

        SearchResponse response = client().prepareSearch("logs").setQuery(QueryBuilders.rangeQuery("timestamp").lte("2024-03-31")).get();

        assertEquals(2, response.getHits().getTotalHits().value);
    }

    public void testRangeQueryWithExplicitRounding() {
        createIndex("logs");
        long now = System.currentTimeMillis();
        ZonedDateTime nowZdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.of("UTC"));
        ZonedDateTime startOfToday = nowZdt.toLocalDate().atStartOfDay(ZoneId.of("UTC"));

        indexDoc("logs", "1", "{\"timestamp\": " + (startOfToday.toInstant().toEpochMilli() - 86400000) + "}");
        indexDoc("logs", "2", "{\"timestamp\": " + startOfToday.toInstant().toEpochMilli() + "}");
        indexDoc("logs", "3", "{\"timestamp\": " + now + "}");
        refresh("logs");

        SearchResponse response = client().prepareSearch("logs").setQuery(QueryBuilders.rangeQuery("timestamp").gte("now/d")).get();

        assertTrue(response.getHits().getTotalHits().value >= 2);
    }

    public void testRangeQueryWithTimezone() {
        createIndex("events");
        indexDoc("events", "1", "{\"created\": \"2022-01-01T05:00:00Z\"}");
        indexDoc("events", "2", "{\"created\": \"2022-01-01T10:00:00Z\"}");
        refresh("events");

        SearchResponse response = client().prepareSearch("events")
            .setQuery(QueryBuilders.rangeQuery("created").gte("2022-01-01").timeZone("America/New_York"))
            .get();

        assertEquals(2, response.getHits().getTotalHits().value);
    }

    public void testRangeQueryBothBounds() {
        createIndex("products");
        indexDoc("products", "1", "{\"price\": 50}");
        indexDoc("products", "2", "{\"price\": 150}");
        indexDoc("products", "3", "{\"price\": 250}");
        indexDoc("products", "4", "{\"price\": 350}");
        refresh("products");

        SearchResponse response = client().prepareSearch("products").setQuery(QueryBuilders.rangeQuery("price").gte(100).lte(300)).get();

        assertEquals(2, response.getHits().getTotalHits().value);
    }

    public void testRangeQueryGtAndLt() {
        createIndex("products");
        indexDoc("products", "1", "{\"price\": 100}");
        indexDoc("products", "2", "{\"price\": 150}");
        indexDoc("products", "3", "{\"price\": 200}");
        refresh("products");

        SearchResponse response = client().prepareSearch("products").setQuery(QueryBuilders.rangeQuery("price").gt(100).lt(200)).get();

        assertEquals(1, response.getHits().getTotalHits().value);
    }

    public void testRangeQueryWithRelation() {
        createIndex("events");
        indexDoc("events", "1", "{\"created\": \"2022-01-15T10:00:00Z\"}");
        indexDoc("events", "2", "{\"created\": \"2022-02-15T10:00:00Z\"}");
        refresh("events");

        SearchResponse response = client().prepareSearch("events")
            .setQuery(QueryBuilders.rangeQuery("created").gte("2022-01-01").lte("2022-01-31").relation("INTERSECTS"))
            .get();

        assertEquals(1, response.getHits().getTotalHits().value);
    }
}
