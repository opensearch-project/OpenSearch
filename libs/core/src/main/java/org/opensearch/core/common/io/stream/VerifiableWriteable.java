/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.common.io.stream;

import java.io.IOException;

/**
 * Provides a method for serialization which will give ordered stream, creating same byte array on every invocation.
 * This should be invoked with a stream that provides ordered serialization.
 */
public interface VerifiableWriteable extends Writeable {

    void writeVerifiableTo(StreamOutput out) throws IOException;
}
