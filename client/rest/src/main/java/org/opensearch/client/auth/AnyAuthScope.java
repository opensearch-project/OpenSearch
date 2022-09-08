/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client.auth;

import org.apache.hc.client5.http.auth.AuthScope;

public class AnyAuthScope extends AuthScope {
    public AnyAuthScope() {
        super("", "", 0, "", "");
    }

    @Override
    public int match(AuthScope that) {
        return 0;
    }
}
