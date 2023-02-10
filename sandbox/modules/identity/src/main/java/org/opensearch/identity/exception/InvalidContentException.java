/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.exception;

import org.opensearch.OpenSearchException;

public class InvalidContentException extends OpenSearchException {

    private static final long serialVersionUID = 1L;

    public InvalidContentException(String msg, Object... args) {
        super(msg, args);
    }

}
