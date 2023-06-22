/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

import java.io.IOException;
import org.opensearch.OpenSearchException;
import org.opensearch.common.io.stream.StreamInput;

public class UnknownApplicationException extends OpenSearchException {

    public UnknownApplicationException(String msg) {
        super(msg);
    }

    public UnknownApplicationException(StreamInput in) throws IOException {
        super(in);
    }

}
