/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.settings;

public class GenericSettingUpgrader<T> implements SettingUpgrader<T> {
    protected final Setting<T> from;
    protected final Setting<T> to;

    public GenericSettingUpgrader(Setting<T> from, Setting<T> to) {
        this.from = from;
        this.to = to;
    }

    @Override
    public Setting<T> getSetting() {
        return this.from;
    }

    @Override
    public String getKey(final String key) {
        return to.getKey();
    }
}
