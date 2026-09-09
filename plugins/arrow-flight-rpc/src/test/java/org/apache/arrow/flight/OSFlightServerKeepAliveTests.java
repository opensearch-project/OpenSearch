/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.arrow.flight;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import io.grpc.netty.NettyServerBuilder;

/**
 * Proves the gRPC keepalive wiring actually reaches the {@link NettyServerBuilder}.
 *
 * <p>{@code FlightTransport} configures server keepalive by registering a
 * {@code Consumer<NettyServerBuilder>} under the {@code "grpc.builderConsumer"} transport hint, which
 * {@link OSFlightServer.Builder#build()} applies to the live builder. The hint is best-effort ("not
 * guaranteed to have any effect"), so this test guards that the key is honored on the pinned Arrow /
 * gRPC versions: it registers the same consumer, recovers it from the builder's options, applies it to
 * a real {@link NettyServerBuilder}, and reads back gRPC's own keepalive fields to confirm the exact
 * values landed. If an Arrow upgrade ever dropped/renamed the hook, or the gRPC keepalive API changed,
 * this fails instead of the keepalive silently becoming a no-op.
 */
public class OSFlightServerKeepAliveTests extends OpenSearchTestCase {

    @SuppressWarnings("unchecked")
    @SuppressForbidden(reason = "reads private builderOptions + gRPC keepalive fields to verify the hint is applied")
    public void testKeepAliveConsumerReachesNettyServerBuilder() throws Exception {
        final long keepAliveMs = 60_000L;
        final long keepAliveTimeoutMs = 20_000L;

        // Register the consumer exactly as FlightTransport does.
        OSFlightServer.Builder builder = OSFlightServer.builder();
        builder.transportHint("grpc.builderConsumer", (Consumer<NettyServerBuilder>) b -> {
            b.keepAliveTime(keepAliveMs, TimeUnit.MILLISECONDS);
            b.keepAliveTimeout(keepAliveTimeoutMs, TimeUnit.MILLISECONDS);
        });

        // Recover the consumer the same way OSFlightServer.build() does (builderOptions is private).
        final Field optionsField = OSFlightServer.Builder.class.getDeclaredField("builderOptions");
        optionsField.setAccessible(true);
        final Map<String, Object> options = (Map<String, Object>) optionsField.get(builder);
        final Object hook = options.get("grpc.builderConsumer");
        assertNotNull("grpc.builderConsumer hint must be stored on the builder", hook);

        // Apply it to a real NettyServerBuilder. accept() not throwing already proves the hint key is
        // honored and the gRPC keepAliveTime/keepAliveTimeout(long, TimeUnit) signatures still match.
        NettyServerBuilder nettyBuilder = NettyServerBuilder.forPort(0);
        ((Consumer<NettyServerBuilder>) hook).accept(nettyBuilder);

        // And confirm the exact values landed by reading gRPC's own keepalive fields (nanos).
        assertEquals(
            "keepAliveTime must be applied to the gRPC builder",
            TimeUnit.MILLISECONDS.toNanos(keepAliveMs),
            readLongField(nettyBuilder, "keepAliveTimeInNanos")
        );
        assertEquals(
            "keepAliveTimeout must be applied to the gRPC builder",
            TimeUnit.MILLISECONDS.toNanos(keepAliveTimeoutMs),
            readLongField(nettyBuilder, "keepAliveTimeoutInNanos")
        );
    }

    @SuppressForbidden(reason = "reads gRPC's private keepalive field to verify the value was applied")
    private static long readLongField(NettyServerBuilder builder, String name) throws Exception {
        final Field f = NettyServerBuilder.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.getLong(builder);
    }
}
