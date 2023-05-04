/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.noop;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.identity.ServiceAccountManager;
import org.opensearch.identity.tokens.AuthToken;

public class NoopServiceAccountManager implements ServiceAccountManager {

    @Override
    public AuthToken resetServiceAccountToken(String principal) {
        return new AuthToken() {
            @Override
            public String toString() {
                return "";
            }
        };
    }

    @Override
    public Boolean isValidToken(AuthToken token) {
        return true;
    }

    @Override
    public void updateServiceAccount(ObjectNode contentAsNode) {}
}
