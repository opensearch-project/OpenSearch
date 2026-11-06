/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.Nullable;
import org.opensearch.common.SuppressForbidden;

import java.lang.reflect.Field;

import io.grpc.ServerCall;
import io.grpc.Status;
import io.grpc.internal.ServerStream;

/**
 * Resets the HTTP/2 stream of a server call. Closing a call only queues its trailers behind the messages its client has not
 * read. Those messages hold their memory for as long as the connection lives, and a reset discards them. gRPC resets a
 * server stream, through {@code ServerStream#cancel}, only when the call's deadline expires, and has no API to reset it
 * otherwise.
 * <p>
 * The stream is a grpc-java internal of the call. The lookup is verified once, and {@link #of} returns {@code null} when it
 * is unavailable.
 */
public final class StreamReset {

    private static final Logger logger = LogManager.getLogger(StreamReset.class);

    static final String SERVER_CALL_CLASS = "io.grpc.internal.ServerCallImpl";

    private static final Status RESET = Status.CANCELLED.withDescription("the client did not read the stream");
    private static final Internals INTERNALS = Internals.lookup();

    private StreamReset() {}

    /**
     * Returns whether the grpc-java internals that this class depends on were found.
     */
    public static boolean isAvailable() {
        return INTERNALS != null;
    }

    /**
     * Returns a task that resets the stream of {@code call}.
     *
     * @param call a call as gRPC passes it to the method's handler
     * @return a task that resets the stream, safe to run from any thread and more than once; {@code null} if the internals
     *         are unavailable or {@code call} is not gRPC's own, for example because an interceptor wrapped it
     */
    @Nullable
    public static Runnable of(ServerCall<?, ?> call) {
        final Internals internals = INTERNALS;
        if (internals == null || call.getClass() != internals.serverCallClass) {
            return null;
        }
        final ServerStream stream;
        try {
            stream = (ServerStream) internals.stream.get(call);
        } catch (ReflectiveOperationException | RuntimeException e) {
            logger.debug("failed to look up the stream of a gRPC call", e);
            return null;
        }
        return () -> stream.cancel(RESET);
    }

    private record Internals(Class<?> serverCallClass, Field stream) {

        @SuppressForbidden(reason = "binds to grpc-java internals to reset a stream its client stopped reading")
        static Internals lookup() {
            try {
                final Class<?> serverCallClass = Class.forName(SERVER_CALL_CLASS, false, StreamReset.class.getClassLoader());
                final Field stream = serverCallClass.getDeclaredField("stream");
                if (stream.getType() != ServerStream.class) {
                    throw new NoSuchFieldException("unexpected type of " + SERVER_CALL_CLASS + ".stream");
                }
                stream.setAccessible(true);
                return new Internals(serverCallClass, stream);
            } catch (ReflectiveOperationException | RuntimeException e) {
                logger.warn("gRPC stream resets are unavailable; a client that stops reading holds its last messages", e);
                return null;
            }
        }
    }
}
