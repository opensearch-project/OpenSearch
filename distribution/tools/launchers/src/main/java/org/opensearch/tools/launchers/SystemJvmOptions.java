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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.tools.launchers;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

final class SystemJvmOptions {

    static List<String> systemJvmOptions() {
        return Collections.unmodifiableList(
            Arrays.asList(
                /*
                 * Cache ttl in seconds for positive DNS lookups noting that this overrides the JDK security property
                 * networkaddress.cache.ttl; can be set to -1 to cache forever.
                 */
                "-Dopensearch.networkaddress.cache.ttl=60",
                /*
                 * Cache ttl in seconds for negative DNS lookups noting that this overrides the JDK security property
                 * networkaddress.cache.negative ttl; set to -1 to cache forever.
                 */
                "-Dopensearch.networkaddress.cache.negative.ttl=10",
                // pre-touch JVM emory pages during initialization
                "-XX:+AlwaysPreTouch",
                // explicitly set the stack size
                "-Xss1m",
                // set to headless, just in case,
                "-Djava.awt.headless=true",
                // ensure UTF-8 encoding by default (e.g., filenames)
                "-Dfile.encoding=UTF-8",
                // use our provided JNA always versus the system one
                "-Djna.nosys=true",
                /*
                 * Turn off a JDK optimization that throws away stack traces for common exceptions because stack traces are important for
                 * debugging.
                 */
                "-XX:-OmitStackTraceInFastThrow",
                // enable helpful NullPointerExceptions (https://openjdk.java.net/jeps/358), if they are supported
                maybeShowCodeDetailsInExceptionMessages(),
                // flags to configure Netty
                "-Dio.netty.noUnsafe=true",
                "-Dio.netty.noKeySetOptimization=true",
                "-Dio.netty.recycler.maxCapacityPerThread=0",
                "-Dio.netty.allocator.numDirectArenas=0",
                // log4j 2
                "-Dlog4j.shutdownHookEnabled=false",
                "-Dlog4j2.disable.jmx=true",
                // security manager
                allowSecurityManagerOption(),
                javaLocaleProviders()
            )
        ).stream().filter(e -> e.isEmpty() == false).collect(Collectors.toList());
    }

    private static String allowSecurityManagerOption() {
        if (Runtime.version().feature() > 17) {
            return "-Djava.security.manager=allow";
        } else {
            return "";
        }
    }

    private static String maybeShowCodeDetailsInExceptionMessages() {
        if (Runtime.version().feature() >= 14) {
            return "-XX:+ShowCodeDetailsInExceptionMessages";
        } else {
            return "";
        }
    }

    private static String javaLocaleProviders() {
        /*
           SPI setting is used to allow loading custom CalendarDataProvider
           in jdk8 it has to be loaded from jre/lib/ext,
           in jdk9+ it is already within ES project and on a classpath
         */
        return "-Djava.locale.providers=SPI,CLDR";
    }

}
