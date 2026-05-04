/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.stream;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.test.rest.yaml.ClientYamlTestCandidate;
import org.opensearch.test.rest.yaml.OpenSearchClientYamlSuiteTestCase;

@AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/pull/21426")
public class StreamTransportClientYamlTestSuiteIT extends OpenSearchClientYamlSuiteTestCase {

    public StreamTransportClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return OpenSearchClientYamlSuiteTestCase.createParameters();
    }
}
