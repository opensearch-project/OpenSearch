/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kinesis;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;

public class KinesisConsumerFactoryTests extends OpenSearchTestCase {
    public void testConstructor() {
        KinesisConsumerFactory factory = new KinesisConsumerFactory();
        Assert.assertNotNull("Factory should be created", factory);
    }

    public void testCreateShardConsumerWithNullSource() {
        KinesisConsumerFactory factory = new KinesisConsumerFactory();
        expectThrows(NullPointerException.class, () -> factory.createShardConsumer("test-client", 0, null));
    }

    public void testParsePointerFromString() {
        KinesisConsumerFactory factory = new KinesisConsumerFactory();
        SequenceNumber sequenceNumber = factory.parsePointerFromString("12345");

        Assert.assertNotNull("Sequence number should be parsed", sequenceNumber);
        Assert.assertEquals("Sequence number should be correctly parsed", "12345", sequenceNumber.getSequenceNumber());
    }
}
