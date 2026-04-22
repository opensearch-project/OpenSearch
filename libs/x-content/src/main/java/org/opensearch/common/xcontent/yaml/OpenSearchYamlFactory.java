/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.xcontent.yaml;

import java.io.InputStream;
import java.io.Reader;

import tools.jackson.core.ObjectReadContext;
import tools.jackson.core.io.IOContext;
import tools.jackson.dataformat.yaml.YAMLFactory;
import tools.jackson.dataformat.yaml.YAMLFactoryBuilder;
import tools.jackson.dataformat.yaml.YAMLParser;

/**
 * Custom {@link YAMLFactory} that creates {@link OpenSearchYamlParser} instances
 * to restore YAML 1.1 boolean handling lost in the Jackson 3.x migration.
 */
class OpenSearchYamlFactory extends YAMLFactory {

    OpenSearchYamlFactory(YAMLFactoryBuilder builder) {
        super(builder);
    }

    @Override
    protected YAMLParser _createParser(ObjectReadContext readCtxt, IOContext ioCtxt, Reader r) {
        return new OpenSearchYamlParser(
            readCtxt,
            ioCtxt,
            _getBufferRecycler(),
            readCtxt.getStreamReadFeatures(_streamReadFeatures),
            readCtxt.getFormatReadFeatures(_formatReadFeatures),
            _loadSettings,
            r
        );
    }

    @Override
    protected YAMLParser _createParser(ObjectReadContext readCtxt, IOContext ioCtxt, InputStream is) {
        return _createParser(readCtxt, ioCtxt, _createReader(is, null, ioCtxt));
    }
}
