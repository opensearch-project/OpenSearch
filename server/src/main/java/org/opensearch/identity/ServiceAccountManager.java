/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.identity.tokens.AuthToken;

/**
 * ServiceAccountManager hooks into security plugin endpoint
 */
public interface ServiceAccountManager {

    public AuthToken resetServiceAccountToken(String principal);

    public Boolean isValidToken(AuthToken token);

    public void updateServiceAccount(ObjectNode contentAsNode);

}
