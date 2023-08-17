/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.io.stream;

import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.time.DateUtils;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable.WriteableRegistry;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadableInstant;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * This utility class registers generic types for streaming over the wire using
 * {@linkplain StreamOutput#writeGenericValue(Object)} and {@linkplain StreamInput#readGenericValue()}
 *
 * In this manner we can register any type across OpenSearch modules, plugins, or libraries without requiring
 * the implementation reside in the server module.
 *
 * @opensearch.internal
 */
public final class Streamables {

    // no instance:
    private Streamables() {}

    /**
     * Called when {@linkplain org.opensearch.transport.TransportService} is loaded by the classloader
     * We do this because streamables depend on the TransportService being loaded
     */
    public static void registerStreamables() {
        registerWriters();
        registerReaders();
    }

    /**
     * Registers writers by class type
     */
    private static void registerWriters() {
        /** {@link ReadableInstant} */
        WriteableRegistry.registerWriter(ReadableInstant.class, (o, v) -> {
            o.writeByte((byte) 13);
            final ReadableInstant instant = (ReadableInstant) v;
            o.writeString(instant.getZone().getID());
            o.writeLong(instant.getMillis());
        });
        WriteableRegistry.registerClassAlias(ReadableInstant.class, ReadableInstant.class);
        /** {@link GeoPoint} */
        WriteableRegistry.registerWriter(GeoPoint.class, (o, v) -> {
            o.writeByte((byte) 22);
            ((GeoPoint) v).writeTo(o);
        });
    }

    /**
     * Registers a reader function mapped by ordinal values that are written by {@linkplain StreamOutput}
     *
     * NOTE: see {@code StreamOutput#WRITERS} for all registered ordinals
     */
    private static void registerReaders() {
        /** {@link ZonedDateTime} */
        WriteableRegistry.registerReader(Byte.valueOf((byte) 13), (i) -> {
            final ZoneId zoneId = DateUtils.dateTimeZoneToZoneId(DateTimeZone.forID(i.readString()));
            long millis = i.readLong();
            return ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), zoneId);
        });
        /** {@link GeoPoint} */
        WriteableRegistry.registerReader(Byte.valueOf((byte) 22), GeoPoint::new);
    }
}
