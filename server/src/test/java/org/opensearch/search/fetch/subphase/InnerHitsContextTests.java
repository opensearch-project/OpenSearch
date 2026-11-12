/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.fetch.subphase;

import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InnerHitsContextTests extends OpenSearchTestCase {

    public void testCopyProducesIndependentDefinitionInstances() {
        InnerHitsContext.InnerHitSubContext original = mock(InnerHitsContext.InnerHitSubContext.class);
        when(original.getName()).thenReturn("inner");
        InnerHitsContext.InnerHitSubContext copiedValue = mock(InnerHitsContext.InnerHitSubContext.class);
        when(original.copy()).thenReturn(copiedValue);

        InnerHitsContext context = new InnerHitsContext();
        context.addInnerHitDefinition(original);

        InnerHitsContext copy = context.copy();

        assertNotSame(context, copy);
        assertNotSame(context.getInnerHits(), copy.getInnerHits());
        assertSame(copiedValue, copy.getInnerHits().get("inner"));
        verify(original).copy();
    }

    public void testCopyKeepsDefinitionNames() {
        InnerHitsContext.InnerHitSubContext original = mock(InnerHitsContext.InnerHitSubContext.class);
        when(original.getName()).thenReturn("innerA");
        InnerHitsContext.InnerHitSubContext copiedValue = mock(InnerHitsContext.InnerHitSubContext.class);
        when(original.copy()).thenReturn(copiedValue);

        InnerHitsContext context = new InnerHitsContext();
        context.addInnerHitDefinition(original);

        InnerHitsContext copy = context.copy();

        assertTrue(copy.getInnerHits().containsKey("innerA"));
        assertEquals(1, copy.getInnerHits().size());
    }

    public void testCopyOfEmptyContextIsEmpty() {
        InnerHitsContext context = new InnerHitsContext();
        InnerHitsContext copy = context.copy();
        assertNotSame(context, copy);
        assertTrue(copy.getInnerHits().isEmpty());
    }
}