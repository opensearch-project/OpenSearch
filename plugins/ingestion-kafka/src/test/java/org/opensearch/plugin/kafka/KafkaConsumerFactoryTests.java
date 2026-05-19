/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;

public class KafkaConsumerFactoryTests extends OpenSearchTestCase {
    public void testCreateShardConsumerWithNullSource() {
        KafkaConsumerFactory factory = new KafkaConsumerFactory();
        expectThrows(NullPointerException.class, () -> factory.createShardConsumer("test-client", 0, null));
    }

    public void testParsePointerFromString() {
        KafkaConsumerFactory factory = new KafkaConsumerFactory();
        KafkaOffset offset = factory.parsePointerFromString("12345");

        Assert.assertNotNull("Offset should be parsed", offset);
        Assert.assertEquals("Offset value should be correctly parsed", 12345L, offset.getOffset());
    }
}
