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

package org.opensearch.common.xcontent;

import org.apache.lucene.util.BytesRef;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentBuilderExtension;

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * SPI extensions for OpenSearch-specific classes (like the Lucene or Joda
 * dependency classes) that need to be encoded by {@link XContentBuilder} in a
 * specific way.
 *
 * @opensearch.internal
 */
public class XContentOpenSearchExtension implements XContentBuilderExtension {

    public static final DateFormatter DEFAULT_FORMATTER = DateFormatter.forPattern("strict_date_optional_time_nanos");
    public static final DateFormatter LOCAL_TIME_FORMATTER = DateFormatter.forPattern("HH:mm:ss.SSS");
    public static final DateFormatter OFFSET_TIME_FORMATTER = DateFormatter.forPattern("HH:mm:ss.SSSZZZZZ");

    @Override
    public Map<Class<?>, XContentBuilder.Writer> getXContentWriters() {
        Map<Class<?>, XContentBuilder.Writer> writers = new HashMap<>();

        // Fully-qualified here to reduce ambiguity around our (OpenSearch') Version class
        writers.put(org.apache.lucene.util.Version.class, (b, v) -> b.value(Objects.toString(v)));
        writers.put(TimeValue.class, (b, v) -> b.value(v.toString()));
        writers.put(ZonedDateTime.class, XContentBuilder::timeValue);
        writers.put(OffsetDateTime.class, XContentBuilder::timeValue);
        writers.put(OffsetTime.class, XContentBuilder::timeValue);
        writers.put(java.time.Instant.class, XContentBuilder::timeValue);
        writers.put(LocalDateTime.class, XContentBuilder::timeValue);
        writers.put(LocalDate.class, XContentBuilder::timeValue);
        writers.put(LocalTime.class, XContentBuilder::timeValue);
        writers.put(DayOfWeek.class, (b, v) -> b.value(v.toString()));
        writers.put(Month.class, (b, v) -> b.value(v.toString()));
        writers.put(MonthDay.class, (b, v) -> b.value(v.toString()));
        writers.put(Year.class, (b, v) -> b.value(v.toString()));
        writers.put(Duration.class, (b, v) -> b.value(v.toString()));
        writers.put(Period.class, (b, v) -> b.value(v.toString()));

        writers.put(BytesReference.class, (b, v) -> {
            if (v == null) {
                b.nullValue();
            } else {
                BytesRef bytes = ((BytesReference) v).toBytesRef();
                b.value(bytes.bytes, bytes.offset, bytes.length);
            }
        });

        writers.put(BytesRef.class, (b, v) -> {
            if (v == null) {
                b.nullValue();
            } else {
                BytesRef bytes = (BytesRef) v;
                b.value(bytes.bytes, bytes.offset, bytes.length);
            }
        });
        return writers;
    }

    @Override
    public Map<Class<?>, XContentBuilder.HumanReadableTransformer> getXContentHumanReadableTransformers() {
        Map<Class<?>, XContentBuilder.HumanReadableTransformer> transformers = new HashMap<>();
        transformers.put(TimeValue.class, v -> ((TimeValue) v).millis());
        transformers.put(ByteSizeValue.class, v -> ((ByteSizeValue) v).getBytes());
        return transformers;
    }

    @Override
    public Map<Class<?>, Function<Object, Object>> getDateTransformers() {
        Map<Class<?>, Function<Object, Object>> transformers = new HashMap<>();
        transformers.put(Date.class, d -> DEFAULT_FORMATTER.format(((Date) d).toInstant()));
        transformers.put(Long.class, d -> DEFAULT_FORMATTER.format(Instant.ofEpochMilli((long) d)));
        transformers.put(Calendar.class, d -> DEFAULT_FORMATTER.format(((Calendar) d).toInstant()));
        transformers.put(GregorianCalendar.class, d -> DEFAULT_FORMATTER.format(((Calendar) d).toInstant()));
        transformers.put(Instant.class, d -> DEFAULT_FORMATTER.format((Instant) d));
        transformers.put(ZonedDateTime.class, d -> DEFAULT_FORMATTER.format((ZonedDateTime) d));
        transformers.put(OffsetDateTime.class, d -> DEFAULT_FORMATTER.format((OffsetDateTime) d));
        transformers.put(OffsetTime.class, d -> OFFSET_TIME_FORMATTER.format((OffsetTime) d));
        transformers.put(LocalDateTime.class, d -> DEFAULT_FORMATTER.format((LocalDateTime) d));
        transformers.put(
            java.time.Instant.class,
            d -> DEFAULT_FORMATTER.format(ZonedDateTime.ofInstant((java.time.Instant) d, ZoneOffset.UTC))
        );
        transformers.put(LocalDate.class, d -> ((LocalDate) d).toString());
        transformers.put(LocalTime.class, d -> LOCAL_TIME_FORMATTER.format((LocalTime) d));
        return transformers;
    }
}
