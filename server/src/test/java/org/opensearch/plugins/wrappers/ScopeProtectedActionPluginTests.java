/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins.wrappers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.junit.Before;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.TransportGetAction;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.common.settings.Settings;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.extensions.ExtensionsSettings;
import org.opensearch.identity.ApplicationAwareSubject;
import org.opensearch.identity.ApplicationManager;
import org.opensearch.identity.IdentityService;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.wrappers.ScopeProtectedActionPlugin;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class ScopeProtectedActionPluginTests extends OpenSearchTestCase {

    private IdentityService identityService;
    private ActionPlugin actionPlugin;
    private ApplicationAwareSubject appSubject;

    @Before
    public void setup() throws IOException {
        identityService = mock(IdentityService.class);
        actionPlugin = mock(ActionPlugin.class);
    }

    public void testExtensionPointProtectedSubjectShouldPass() {
        final List<ActionFilter> expectedActions = List.of(mock(ActionFilter.class));
        when(actionPlugin.getActionFilters()).thenReturn(expectedActions);
        final List<ActionFilter> getActionsFilters = actionPlugin.getActionFilters();
        verify(actionPlugin).getActionFilters();
        assertThat(getActionsFilters, equalTo(expectedActions));

        final ActionPlugin scopeProtectedActionPlugin = new ScopeProtectedActionPlugin(null, actionPlugin, identityService);
        final ApplicationAwareSubject subject = mock(ApplicationAwareSubject.class);
        when(identityService.getSubject()).thenReturn(subject);
        when(subject.isAllowed(any())).thenReturn(true);

        final List<ActionFilter> getActionsProtecedFilters = scopeProtectedActionPlugin.getActionFilters();
        assertThat(getActionsProtecedFilters, equalTo(expectedActions));

        verify(actionPlugin, times(2)).getActionFilters();
        verifyNoMoreInteractions(actionPlugin);
        verify(identityService).getSubject();
        verify(subject).isAllowed(any());
    }

    public void testExtensionPointProtectedSubjectShouldFail() {
        final ActionPlugin scopeProtectedActionPlugin = new ScopeProtectedActionPlugin(null, actionPlugin, identityService);
        final ApplicationAwareSubject subject = mock(ApplicationAwareSubject.class);
        when(identityService.getSubject()).thenReturn(subject);
        when(subject.isAllowed(any())).thenReturn(false);

        final Exception exception = assertThrows(OpenSearchException.class, scopeProtectedActionPlugin::getActionFilters);
        assertThat(exception.getMessage(), containsString("EXTENSION_POINT.ACTION_PLUGIN.IMPLEMENT"));

        verifyNoInteractions(actionPlugin);
        verify(identityService).getSubject();
        verify(subject).isAllowed(any());
    }
}
