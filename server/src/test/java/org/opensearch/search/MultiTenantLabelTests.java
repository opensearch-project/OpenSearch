/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class MultiTenantLabelTests extends AbstractSearchTestCase {

    public void testValidMultiTenantLabel() {
        MultiTenantLabel label = MultiTenantLabel.fromName("tenant");
        assertEquals(label.getValue(), "tenant");
    }

    public void testInvalidMultiTenantLabel() {
        assertThrows(IllegalArgumentException.class, () -> MultiTenantLabel.fromName("foo"));
    }

    public void testValidMultiTenantLabelWithStreamInput() throws IOException {
        StreamInput streamInput = mock(StreamInput.class);
        doReturn("tenant").when(streamInput).readString();

        MultiTenantLabel label = MultiTenantLabel.fromName(streamInput);
        assertEquals(label.getValue(), "tenant");
    }

    public void testInvalidMultiTenantLabelWithStreamInput() throws IOException {
        StreamInput streamInput = mock(StreamInput.class);
        doReturn("foo").when(streamInput).readString();

        assertThrows(IllegalArgumentException.class, () -> MultiTenantLabel.fromName(streamInput));
    }

}
