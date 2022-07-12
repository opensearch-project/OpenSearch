/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import org.opensearch.OpenSearchException;
import org.opensearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Exception raised from master node due to task throttling.
 */
public class MasterTaskThrottlingException extends OpenSearchException {

    public MasterTaskThrottlingException(String msg, Object... args) {
        super(msg, args);
    }

    public MasterTaskThrottlingException(StreamInput in) throws IOException {
        super(in);
    }
}
