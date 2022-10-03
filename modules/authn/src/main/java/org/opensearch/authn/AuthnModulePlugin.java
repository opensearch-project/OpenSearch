/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn;

import org.opensearch.common.settings.Setting;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SystemIndexPlugin;

import java.util.Arrays;
import java.util.List;

public class AuthnModulePlugin extends Plugin implements SystemIndexPlugin, NetworkPlugin {
    public static final Setting<String> INTERNAL_REALM_NAME = Setting.simpleString(
        "authn.realm.name",
        "internal",
        Setting.Property.NodeScope
    );

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(INTERNAL_REALM_NAME);
    }
}
