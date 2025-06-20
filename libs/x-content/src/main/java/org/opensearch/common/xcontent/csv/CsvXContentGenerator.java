/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.xcontent.csv;

import com.fasterxml.jackson.core.JsonGenerator;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContentGenerator;

import java.io.OutputStream;
import java.util.Set;

public class CsvXContentGenerator extends JsonXContentGenerator {

    public CsvXContentGenerator(JsonGenerator jsonGenerator, OutputStream os, Set<String> includes, Set<String> excludes) {
        super(jsonGenerator, os, includes, excludes);
    }

    @Override
    public XContentType contentType() {
        return XContentType.CSV;
    }

    @Override
    public void usePrintLineFeedAtEnd() {
        // nothing here
    }

    @Override
    protected boolean supportsRawWrites() {
        return false;
    }

}
