/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.Text;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Reads Arrow vector cells as plain Java values, unwrapping Arrow {@link Text} recursively. */
public final class ArrowValues {

    private ArrowValues() {}

    public static Object toJavaValue(FieldVector vector, int index) {
        if (vector.isNull(index)) return null;
        if (vector instanceof VarCharVector v) {
            return new String(v.get(index), StandardCharsets.UTF_8);
        }
        // MapVector extends ListVector — must come first.
        if (vector instanceof MapVector && vector.getObject(index) instanceof List<?> entries) {
            LinkedHashMap<String, Object> map = new LinkedHashMap<>();
            for (Object entry : entries) {
                if (!(entry instanceof Map<?, ?> e)) continue;
                Object k = e.get(MapVector.KEY_NAME);
                Object v = e.get(MapVector.VALUE_NAME);
                map.put(k instanceof Text t ? t.toString() : String.valueOf(k), normalize(v));
            }
            return map;
        }
        Object value = vector.getObject(index);
        if (vector instanceof ListVector && value instanceof List<?> raw) {
            return normalizeList(raw);
        }
        // Date/Time/Timestamp cells return java.time.* so ExprValueUtils wraps them as
        // ExprDateValue / ExprTimeValue / ExprTimestampValue and formats them.
        Object temporal = normalizeTemporal(vector.getField().getType(), value);
        if (temporal != null) {
            return temporal;
        }
        return normalize(value);
    }

    /** Returns LocalDate / LocalTime / LocalDateTime, or null for non-temporal types. */
    private static Object normalizeTemporal(ArrowType type, Object value) {
        if (value == null) return null;
        if (type instanceof ArrowType.Date date) {
            return toLocalDate(date, value);
        }
        if (type instanceof ArrowType.Time time) {
            return toLocalTime(time, value);
        }
        if (type instanceof ArrowType.Timestamp ts) {
            return toLocalDateTime(ts, value);
        }
        return null;
    }

    private static LocalDate toLocalDate(ArrowType.Date type, Object value) {
        if (value instanceof LocalDate ld) return ld;
        if (value instanceof LocalDateTime ldt) return ldt.toLocalDate();
        long raw = ((Number) value).longValue();
        return switch (type.getUnit()) {
            case DAY -> LocalDate.ofEpochDay(raw);
            case MILLISECOND -> LocalDate.ofEpochDay(Math.floorDiv(raw, 86_400_000L));
        };
    }

    private static LocalTime toLocalTime(ArrowType.Time type, Object value) {
        if (value instanceof LocalTime lt) return lt;
        if (value instanceof LocalDateTime ldt) return ldt.toLocalTime();
        long raw = ((Number) value).longValue();
        long nanoOfDay = switch (type.getUnit()) {
            case SECOND -> raw * 1_000_000_000L;
            case MILLISECOND -> raw * 1_000_000L;
            case MICROSECOND -> raw * 1_000L;
            case NANOSECOND -> raw;
        };
        return LocalTime.ofNanoOfDay(nanoOfDay);
    }

    private static LocalDateTime toLocalDateTime(ArrowType.Timestamp type, Object value) {
        if (value instanceof LocalDateTime ldt) return ldt;
        long raw = ((Number) value).longValue();
        Instant instant = switch (type.getUnit()) {
            case SECOND -> Instant.ofEpochSecond(raw);
            case MILLISECOND -> Instant.ofEpochMilli(raw);
            case MICROSECOND -> Instant.ofEpochSecond(Math.floorDiv(raw, 1_000_000L), Math.floorMod(raw, 1_000_000L) * 1_000L);
            case NANOSECOND -> Instant.ofEpochSecond(Math.floorDiv(raw, 1_000_000_000L), Math.floorMod(raw, 1_000_000_000L));
        };
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private static Object normalize(Object value) {
        if (value instanceof Text t) {
            return t.toString();
        }
        if (value instanceof List<?> list) {
            return normalizeList(list);
        }
        if (value instanceof Map<?, ?> m) {
            LinkedHashMap<String, Object> out = new LinkedHashMap<>(m.size());
            for (Map.Entry<?, ?> entry : m.entrySet()) {
                Object k = entry.getKey();
                out.put(k instanceof Text t ? t.toString() : String.valueOf(k), normalize(entry.getValue()));
            }
            return out;
        }
        return value;
    }

    private static List<Object> normalizeList(List<?> raw) {
        List<Object> out = new ArrayList<>(raw.size());
        for (Object element : raw) {
            out.add(normalize(element));
        }
        return out;
    }
}
