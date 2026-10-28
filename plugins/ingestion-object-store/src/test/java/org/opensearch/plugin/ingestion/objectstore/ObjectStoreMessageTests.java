/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Unit tests for {@link ObjectStoreMessage} payload defensive copying and timestamp accessor.
 */
public class ObjectStoreMessageTests extends OpenSearchTestCase {

    public void testGetPayloadReturnsClone() {
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        ObjectStoreMessage message = new ObjectStoreMessage(payload, 42L);
        byte[] returned = message.getPayload();
        assertThat(returned, equalTo(payload));
        assertThat(returned, is(not(sameInstance(payload))));
        returned[0] = 'x';
        assertThat(message.getPayload(), equalTo(payload));
    }

    public void testGetPayloadNull() {
        ObjectStoreMessage message = new ObjectStoreMessage(null, 1L);
        assertThat(message.getPayload(), is(nullValue()));
    }

    public void testGetTimestamp() {
        ObjectStoreMessage message = new ObjectStoreMessage(new byte[] { 1 }, 7L);
        assertThat(message.getTimestamp(), is(7L));
    }
}
