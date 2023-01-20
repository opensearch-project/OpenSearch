/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensible.identity;

import org.opensearch.authn.AccessTokenManager;
import org.opensearch.authn.AuthenticationManager;
import org.opensearch.authn.Subject;
import org.opensearch.common.settings.Setting;
import org.opensearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.List;

public class TestIdentityPlugin extends Plugin {

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<Setting<?>>();
        settings.add(
            Setting.simpleString(
                "extended.plugins",
                ExtensibleIdentityPlugin.class.getCanonicalName(),
                Setting.Property.NodeScope,
                Setting.Property.Filtered
            )
        );

        return settings;
    }

    public class TestAuthenticationManager implements AuthenticationManager {
        @Override
        public Subject getSubject() {
            return null;
        }

        @Override
        public AccessTokenManager getAccessTokenManager() {
            return null;
        }
    }
}
