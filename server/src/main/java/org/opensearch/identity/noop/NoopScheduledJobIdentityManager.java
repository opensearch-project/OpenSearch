/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.noop;

import org.opensearch.identity.ScheduledJobIdentityManager;
import org.opensearch.identity.schedule.ScheduledJobOperator;
import org.opensearch.identity.tokens.AuthToken;

import java.util.Optional;

/**
 * Implementation of subject that is always authenticated
 *
 * This class and related classes in this package will not return nulls or fail permissions checks
 *
 * @opensearch.internal
 */
public class NoopScheduledJobIdentityManager implements ScheduledJobIdentityManager {
    @Override
    public void saveUserDetails(String jobId, String indexName, ScheduledJobOperator operator) {
        return;
    }

    @Override
    public void deleteUserDetails(String jobId, String indexName) {
        return;
    }

    @Override
    public AuthToken issueAccessTokenOnBehalfOfUser(String jobId, String indexName, Optional<String> extensionUniqueId) {
        return new NoopAuthToken();
    }
}
