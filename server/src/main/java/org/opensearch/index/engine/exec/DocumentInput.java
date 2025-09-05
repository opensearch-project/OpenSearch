/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.MappedFieldType;

import java.io.IOException;
@ExperimentalApi
public interface DocumentInput<T> extends AutoCloseable {

    void addField(MappedFieldType fieldType, Object value);

    T getFinalInput();

    WriteResult addToWriter() throws IOException;
}
