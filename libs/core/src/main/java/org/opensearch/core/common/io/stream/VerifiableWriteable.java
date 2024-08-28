/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.common.io.stream;

import java.io.IOException;

public interface VerifiableWriteable extends Writeable {
    void writeVerifiableTo(StreamOutput out) throws IOException;
}
