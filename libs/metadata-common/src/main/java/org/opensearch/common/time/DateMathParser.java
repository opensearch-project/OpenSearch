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

package org.opensearch.common.time;

import org.opensearch.common.annotation.PublicApi;
import org.joda.time.DateTimeZone;

import java.time.Instant;
import java.time.ZoneId;
import java.util.function.LongSupplier;

/**
 * An abstraction over date math parsing to allow different implementation for joda and java time.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface DateMathParser {

    /**
     * Parse a date math expression without timezone info and rounding down.
     */
    default Instant parse(String text, LongSupplier now) {
        return parse(text, now, false, (ZoneId) null);
    }

    // Note: we take a callable here for the timestamp in order to be able to figure out
    // if it has been used. For instance, the request cache does not cache requests that make
    // use of `now`.

    // exists for backcompat, do not use!
    @Deprecated
    default Instant parse(String text, LongSupplier now, boolean roundUpProperty, DateTimeZone tz) {
        return parse(text, now, roundUpProperty, tz == null ? null : ZoneId.of(tz.getID()));
    }

    /**
     * Parse text, that potentially contains date math into the milliseconds since the epoch
     * <p>
     * Examples are
     * <p>
     * <code>2014-11-18||-2y</code> subtracts two years from the input date
     * <code>now/m</code>           rounds the current time to minute granularity
     * <p>
     * Supported rounding units are
     * y    year
     * M    month
     * w    week (beginning on a monday)
     * d    day
     * h/H  hour
     * m    minute
     * s    second
     *
     *
     * @param text              the input
     * @param now               a supplier to retrieve the current date in milliseconds, if needed for additions
     * @param roundUpProperty   should the result be rounded up with the granularity of the rounding (e.g. <code>now/M</code>)
     * @param tz                an optional timezone that should be applied before returning the milliseconds since the epoch
     * @return                  the parsed date as an Instant since the epoch
     */
    Instant parse(String text, LongSupplier now, boolean roundUpProperty, ZoneId tz);
}
