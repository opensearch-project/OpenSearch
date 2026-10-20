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

package org.opensearch.monitor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;

/**
 * Probes the various resources
 *
 * @opensearch.internal
 */
public class Probes {
    private static final Logger logger = LogManager.getLogger(Probes.class);
    private static volatile boolean shouldLogException = true;

    public static short getLoadAndScaleToPercent(Method method, OperatingSystemMXBean osMxBean) {
        if (method != null) {
            try {
                double load = (double) method.invoke(osMxBean);
                if (load >= 0) {
                    return (short) (load * 100);
                }
            } catch (Exception e) {
                // Only log an exception once, ideally it should happen during the bootstrap check only
                // (see please BootstrapChecks.ProbesCheck)
                if (shouldLogException == true) {
                    logger.error("Unable to call method '" + method + "'", e);
                    shouldLogException = false;
                }
                return -1;
            }
        }
        return -1;
    }
}
