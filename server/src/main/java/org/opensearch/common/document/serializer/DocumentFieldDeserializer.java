/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.document.serializer;

import org.opensearch.common.document.DocumentField;

import java.io.IOException;

/**
 * Deserializer for {@link DocumentField} which can be implemented for different types of serde mechanisms.
 */
public interface DocumentFieldDeserializer<T> {

    DocumentField createDocumentField(T inputStream) throws IOException;

}
