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

package org.opensearch.bootstrap;

import org.apache.logging.log4j.LogManager;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;

import java.io.ObjectInputFilter;

/**
 * Settings used for bootstrapping OpenSearch
 *
 * @opensearch.internal
 */
public final class BootstrapSettings {

    private BootstrapSettings() {}

    // TODO: remove this hack when insecure defaults are removed from java
    public static final Setting<Boolean> SECURITY_FILTER_BAD_DEFAULTS_SETTING = Setting.boolSetting(
        "security.manager.filter_bad_defaults",
        true,
        Property.NodeScope
    );

    public static final Setting<Boolean> MEMORY_LOCK_SETTING = Setting.boolSetting("bootstrap.memory_lock", false, Property.NodeScope);
    public static final Setting<Boolean> SYSTEM_CALL_FILTER_SETTING = Setting.boolSetting(
        "bootstrap.system_call_filter",
        true,
        Property.NodeScope
    );
    public static final Setting<Boolean> CTRLHANDLER_SETTING = Setting.boolSetting("bootstrap.ctrlhandler", true, Property.NodeScope);

    public static final Setting<Boolean> SERIAL_FILTER_SETTING = Setting.boolSetting("bootstrap.serial_filter", false, Property.NodeScope);

    static final ObjectInputFilter REJECT_ALL_FILTER = filterInfo -> filterInfo.serialClass() == null
        ? ObjectInputFilter.Status.UNDECIDED
        : ObjectInputFilter.Status.REJECTED;

    /**
     * Installs a process-wide ObjectInputFilter that rejects all Java deserialization by default.
     * Code that needs deserialization can opt in by calling setObjectInputFilter on their stream.
     */
    public static void initializeSerialFilter() {
        try {
            ObjectInputFilter.Config.setSerialFilter(REJECT_ALL_FILTER);
        } catch (IllegalStateException e) {
            LogManager.getLogger(BootstrapSettings.class).debug("Serial filter already initialized", e);
        }
    }

}
