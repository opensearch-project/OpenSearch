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
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/** Reads Arrow vector cells as plain Java values, unwrapping Arrow {@link Text} recursively. */
public final class ArrowValues {

    // Space-separator output matches the SQL plugin's ExprTimestampValue.
    private static final DateTimeFormatter TIMESTAMP_NO_NANO = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT);
    private static final DateTimeFormatter TIMESTAMP_WITH_NANO = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS", Locale.ROOT);
    private static final DateTimeFormatter TIME_NO_NANO = DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ROOT);
    private static final DateTimeFormatter TIME_WITH_NANO = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS", Locale.ROOT);
    // DataFusion CAST(temporal AS VARCHAR) — date and time joined by 'T', optional fraction.
    private static final Pattern ISO_TIMESTAMP_T = Pattern.compile("^(\\d{4}-\\d{2}-\\d{2})T(\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?)$");

    private ArrowValues() {}

    public static Object toJavaValue(FieldVector vector, int index) {
        if (vector.isNull(index)) return null;
        if (vector instanceof VarCharVector v) {
            return spaceSeparator(new String(v.get(index), StandardCharsets.UTF_8));
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
        if (vector instanceof ListVector lv && value instanceof List<?> raw) {
            // child Arrow type drives temporal element formatting
            return normalizeList(raw, lv.getDataVector().getField());
        }
        Object temporal = formatTemporal(vector.getField().getType(), value);
        if (temporal != null) {
            return temporal;
        }
        return normalize(value);
    }

    /** ISO-T temporal → space separator; other strings unchanged. */
    private static String spaceSeparator(String s) {
        if (s == null) return null;
        var m = ISO_TIMESTAMP_T.matcher(s);
        return m.matches() ? m.group(1) + " " + m.group(2) : s;
    }

    private static Object formatTemporal(ArrowType type, Object value) {
        if (value == null) return null;
        if (type instanceof ArrowType.Date date) {
            return formatDate(date, value);
        }
        if (type instanceof ArrowType.Time time) {
            return formatTime(time, value);
        }
        if (type instanceof ArrowType.Timestamp ts) {
            return formatTimestamp(ts, value);
        }
        return null;
    }

    private static String formatDate(ArrowType.Date type, Object value) {
        LocalDate ld;
        if (value instanceof LocalDate d) {
            ld = d;
        } else if (value instanceof LocalDateTime ldt) {
            ld = ldt.toLocalDate();
        } else {
            long raw = ((Number) value).longValue();
            ld = switch (type.getUnit()) {
                case DAY -> LocalDate.ofEpochDay(raw);
                case MILLISECOND -> LocalDate.ofEpochDay(Math.floorDiv(raw, 86_400_000L));
            };
        }
        return ld.format(DateTimeFormatter.ISO_LOCAL_DATE);
    }

    /** Time -> HH:mm:ss[.frac]; never prefixes with the 1970 epoch date. */
    private static String formatTime(ArrowType.Time type, Object value) {
        LocalTime lt;
        if (value instanceof LocalTime t) {
            lt = t;
        } else if (value instanceof LocalDateTime ldt) {
            lt = ldt.toLocalTime();
        } else {
            long raw = ((Number) value).longValue();
            long nanoOfDay = switch (type.getUnit()) {
                case SECOND -> raw * 1_000_000_000L;
                case MILLISECOND -> raw * 1_000_000L;
                case MICROSECOND -> raw * 1_000L;
                case NANOSECOND -> raw;
            };
            lt = LocalTime.ofNanoOfDay(nanoOfDay);
        }
        return lt.getNano() == 0 ? lt.format(TIME_NO_NANO) : lt.format(TIME_WITH_NANO);
    }

    /** Timestamp -> yyyy-MM-dd HH:mm:ss[.frac]. */
    private static String formatTimestamp(ArrowType.Timestamp type, Object value) {
        LocalDateTime ldt;
        if (value instanceof LocalDateTime t) {
            ldt = t;
        } else if (value instanceof LocalDate ld) {
            ldt = ld.atStartOfDay();
        } else {
            long raw = ((Number) value).longValue();
            Instant instant = switch (type.getUnit()) {
                case SECOND -> Instant.ofEpochSecond(raw);
                case MILLISECOND -> Instant.ofEpochMilli(raw);
                case MICROSECOND -> Instant.ofEpochSecond(Math.floorDiv(raw, 1_000_000L), Math.floorMod(raw, 1_000_000L) * 1_000L);
                case NANOSECOND -> Instant.ofEpochSecond(Math.floorDiv(raw, 1_000_000_000L), Math.floorMod(raw, 1_000_000_000L));
            };
            ldt = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        }
        return ldt.getNano() == 0 ? ldt.format(TIMESTAMP_NO_NANO) : ldt.format(TIMESTAMP_WITH_NANO);
    }

    private static Object normalize(Object value) {
        if (value instanceof Text t) {
            return spaceSeparator(t.toString());
        }
        if (value instanceof String s) {
            return spaceSeparator(s);
        }
        if (value instanceof List<?> list) {
            return normalizeList(list, null);
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

    private static List<Object> normalizeList(List<?> raw, Field childField) {
        ArrowType childType = childField == null ? null : childField.getType();
        List<Object> out = new ArrayList<>(raw.size());
        for (Object element : raw) {
            Object formatted = childType == null ? null : formatTemporal(childType, element);
            out.add(formatted != null ? formatted : normalize(element));
        }
        return out;
    }
}
