/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.opensearch.OpenSearchException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;

import java.io.IOException;

public class InvalidIndexContextException extends OpenSearchException {

    public InvalidIndexContextException(String indexName, String name, String description) {
        super("Invalid context name [{}] provide for index: {}, [{}]", name, indexName, description);
    }

    public InvalidIndexContextException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
