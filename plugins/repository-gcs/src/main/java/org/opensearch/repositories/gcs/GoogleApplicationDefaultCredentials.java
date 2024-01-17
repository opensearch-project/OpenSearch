/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.gcs;

import com.google.auth.oauth2.GoogleCredentials;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * This class facilitates to fetch Application Default Credentials
 * see <a href="https://cloud.google.com/docs/authentication/application-default-credentials">How Application Default Credentials works</a>
 */
public class GoogleApplicationDefaultCredentials {
    private static final Logger logger = LogManager.getLogger(GoogleApplicationDefaultCredentials.class);

    public GoogleCredentials get() {
        GoogleCredentials credentials = null;
        try {
            credentials = SocketAccess.doPrivilegedIOException(GoogleCredentials::getApplicationDefault);
        } catch (IOException e) {
            logger.error("Failed to retrieve \"Application Default Credentials\"", e);
        }
        return credentials;
    }
}
