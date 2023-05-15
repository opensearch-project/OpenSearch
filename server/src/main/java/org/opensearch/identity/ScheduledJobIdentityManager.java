/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.opensearch.identity.tokens.AuthToken;

/**
 * An interface with methods used to provide security for scheduled jobs
 *
 * @opensearch.experimental
 */
public interface ScheduledJobIdentityManager {

    /**
     * Method implemented by an identity plugin to store user information for a scheduled job
     * @param jobId The id of the scheduled job
     * @param indexName The index where scheduled job details is stored
     */
    void saveUserDetails(String jobId, String indexName);

    /**
     * Method implemented by an identity plugin to delete user information for a scheduled job
     * @param jobId The id of the scheduled job
     * @param indexName The index where scheduled job details is stored
     */
    void deleteUserDetails(String jobId, String indexName);

    /**
     * Method implemented by an identity plugin to issue an access token for a scheduler job runner
     * @param jobId The id of the scheduled job
     * @param indexName The index where scheduled job details is stored
     */
    AuthToken issueAccessTokenOnBehalfOfUser(String jobId, String indexName);
}
