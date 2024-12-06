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

package org.opensearch.common.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.PublicApi;

/**
 * A logger that logs deprecation notices. Logger should be initialized with a parent logger which name will be used
 * for deprecation logger. For instance <code>DeprecationLogger.getLogger("org.opensearch.test.SomeClass")</code> will
 * result in a deprecation logger with name <code>org.opensearch.deprecation.test.SomeClass</code>. This allows to use a
 * <code>deprecation</code> logger defined in log4j2.properties.
 * <p>
 * Logs are emitted at the custom {@link #DEPRECATION} level, and routed wherever they need to go using log4j. For example,
 * to disk using a rolling file appender, or added as a response header using {@link HeaderWarningAppender}.
 * <p>
 * Deprecation messages include a <code>key</code>, which is used for rate-limiting purposes. The log4j configuration
 * uses {@link RateLimitingFilter} to prevent the same message being logged repeatedly in a short span of time. This
 * key is combined with the <code>X-Opaque-Id</code> request header value, if supplied, which allows for per-client
 * message limiting.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class DeprecationLogger {

    /**
     * Deprecation messages are logged at this level.
     */
    public static Level DEPRECATION = Level.forName("DEPRECATION", Level.WARN.intLevel() + 1);

    private final Logger logger;

    private DeprecationLogger(Logger parentLogger) {
        this.logger = parentLogger;
    }

    /**
     * Creates a new deprecation logger for the supplied class. Internally, it delegates to
     * {@link #getLogger(String)}, passing the full class name.
     */
    public static DeprecationLogger getLogger(Class<?> aClass) {
        return getLogger(toLoggerName(aClass));
    }

    /**
     * Creates a new deprecation logger based on the parent logger. Automatically
     * prefixes the logger name with "deprecation", if it starts with "org.opensearch.",
     * it replaces "org.opensearch" with "org.opensearch.deprecation" to maintain
     * the "org.opensearch" namespace.
     */
    public static DeprecationLogger getLogger(String name) {
        return new DeprecationLogger(getDeprecatedLoggerForName(name));
    }

    private static Logger getDeprecatedLoggerForName(String name) {
        if (name.startsWith("org.opensearch")) {
            name = name.replace("org.opensearch.", "org.opensearch.deprecation.");
        } else {
            name = "deprecation." + name;
        }
        return LogManager.getLogger(name);
    }

    private static String toLoggerName(final Class<?> cls) {
        String canonicalName = cls.getCanonicalName();
        return canonicalName != null ? canonicalName : cls.getName();
    }

    /**
     * Logs a message at the {@link #DEPRECATION} level. The message is also sent to the header warning logger,
     * so that it can be returned to the client.
     */
    public DeprecationLoggerBuilder deprecate(final String key, final String msg, final Object... params) {
        return new DeprecationLoggerBuilder().withDeprecation(key, msg, params);
    }

    /**
     * The builder for the deprecation logger
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public class DeprecationLoggerBuilder {

        public DeprecationLoggerBuilder withDeprecation(String key, String msg, Object[] params) {
            // Check if the logger is enabled to skip the overhead of deduplicating messages if the logger is disabled
            if (logger.isEnabled(DEPRECATION)) {
                DeprecatedMessage deprecationMessage = new DeprecatedMessage(key, HeaderWarning.getXOpaqueId(), msg, params);
                if (!deprecationMessage.isAlreadyLogged()) {
                    logger.log(DEPRECATION, deprecationMessage);
                }
            }
            return this;
        }
    }
}
