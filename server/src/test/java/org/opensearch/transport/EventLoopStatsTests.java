/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.http.HttpStats;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class EventLoopStatsTests extends OpenSearchTestCase {

    public void testAggregatesPerWorkerQueues() {
        EventLoopStats stats = stats();

        assertTrue(stats.isSharedGroup());
        assertThat(stats.getWorkerCount(), equalTo(2));
        assertThat(stats.getPendingTasks(), equalTo(11L));
        assertThat(stats.getMaxPendingTasks(), equalTo(7));
    }

    public void testTransportXContent() throws IOException {
        TransportStats stats = new TransportStats.Builder().serverOpen(2)
            .totalOutboundConnections(1)
            .rxCount(3)
            .rxSize(4)
            .txCount(5)
            .txSize(6)
            .eventLoopStats(stats())
            .build();

        String json = toJson(stats);
        assertThat(json, containsString("\"server_open\":2"));
        assertThat(
            json,
            containsString(
                "\"event_loop\":{\"shared_group\":true,\"worker_count\":2,\"pending_tasks\":11,\"max_pending_tasks\":7,"
                    + "\"workers\":[{\"name\":\"transport_worker[T#1]\",\"pending_tasks\":4},"
                    + "{\"name\":\"transport_worker[T#2]\",\"pending_tasks\":7}]}"
            )
        );
    }

    public void testHttpXContentOmitsEventLoopWhenAbsent() throws IOException {
        HttpStats stats = new HttpStats.Builder().serverOpen(1).totalOpen(2).build();

        String json = toJson(stats);
        assertThat(json, containsString("\"current_open\":1"));
        assertThat(json, not(containsString("event_loop")));
        assertThat(stats.getEventLoopStats(), nullValue());
    }

    public void testRoundTripOnCurrentVersion() throws IOException {
        TransportStats original = transportStats(stats());
        TransportStats deserialized = copyTransport(original, Version.CURRENT);

        assertThat(deserialized.getServerOpen(), equalTo(original.getServerOpen()));
        assertThat(deserialized.getRxCount(), equalTo(original.getRxCount()));
        assertThat(deserialized.getEventLoopStats(), equalTo(original.getEventLoopStats()));

        HttpStats http = new HttpStats.Builder().serverOpen(8).totalOpen(9).eventLoopStats(stats()).build();
        assertThat(copyHttp(http, Version.CURRENT).getEventLoopStats(), equalTo(http.getEventLoopStats()));
    }

    public void testOlderVersionOmitsEventLoopStats() throws IOException {
        TransportStats original = transportStats(stats());
        TransportStats deserialized = copyTransport(original, Version.V_3_9_0);

        assertThat(deserialized.getServerOpen(), equalTo(9L));
        assertThat(deserialized.getTxCount(), equalTo(3L));
        assertThat(deserialized.getEventLoopStats(), nullValue());

        HttpStats http = new HttpStats.Builder().serverOpen(4).totalOpen(5).eventLoopStats(stats()).build();
        HttpStats oldHttp = copyHttp(http, Version.V_3_9_0);
        assertThat(oldHttp.getServerOpen(), equalTo(4L));
        assertThat(oldHttp.getTotalOpen(), equalTo(5L));
        assertThat(oldHttp.getEventLoopStats(), nullValue());
    }

    public void testRejectsInvalidWorkers() {
        expectThrows(IllegalArgumentException.class, () -> new EventLoopStats.Worker("", 1));
        expectThrows(IllegalArgumentException.class, () -> new EventLoopStats.Worker("worker", -1));
        expectThrows(NullPointerException.class, () -> new EventLoopStats(false, null));
    }

    private static EventLoopStats stats() {
        return new EventLoopStats(
            true,
            List.of(new EventLoopStats.Worker("transport_worker[T#1]", 4), new EventLoopStats.Worker("transport_worker[T#2]", 7))
        );
    }

    private static TransportStats transportStats(EventLoopStats eventLoopStats) {
        return new TransportStats.Builder().serverOpen(9)
            .totalOutboundConnections(1)
            .rxCount(2)
            .rxSize(3)
            .txCount(3)
            .txSize(4)
            .eventLoopStats(eventLoopStats)
            .build();
    }

    private static String toJson(ToXContent fragment) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        fragment.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return builder.toString();
    }

    private static TransportStats copyTransport(TransportStats original, Version version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setVersion(version);
                return new TransportStats(in);
            }
        }
    }

    private static HttpStats copyHttp(HttpStats original, Version version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setVersion(version);
                return new HttpStats(in);
            }
        }
    }
}
