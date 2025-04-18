/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.scale.searchonly;

import org.opensearch.Version;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class ScaleIndexOperationValidatorTests extends OpenSearchTestCase {

    private ScaleIndexOperationValidator validator;
    private ActionListener<AcknowledgedResponse> listener;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        validator = new ScaleIndexOperationValidator();
        // Create a mock listener so we can verify onFailure is called with the expected exception.
        listener = Mockito.mock(ActionListener.class);
    }

    public void testValidateScalePrerequisites_NullIndexMetadata() {
        // When index metadata is null, validation should fail.
        boolean result = validator.validateScalePrerequisites(null, "test-index", listener, true);
        assertFalse(result);
        verify(listener).onFailure(argThat(new ExceptionMatcher("Index [test-index] not found")));
    }

    public void testValidateScalePrerequisites_ScaleDown_AlreadySearchOnly() {
        // For scale-down, if the index is already marked as search-only, validation should fail.
        Settings settings = Settings.builder().put(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), true).build();
        IndexMetadata indexMetadata = createTestIndexMetadata("test-index", settings, 1);
        boolean result = validator.validateScalePrerequisites(indexMetadata, "test-index", listener, true);
        assertFalse(result);
        verify(listener).onFailure(argThat(new ExceptionMatcher("already in search-only mode")));
    }

    public void testValidateScalePrerequisites_ScaleDown_NoSearchOnlyReplicas() {
        // If there are zero search-only replicas, validation should fail.
        Settings settings = Settings.builder()
            .put(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, "SEGMENT")
            .build();
        // Pass zero for the number of search-only replicas.
        IndexMetadata indexMetadata = createTestIndexMetadata("test-index", settings, 0);
        boolean result = validator.validateScalePrerequisites(indexMetadata, "test-index", listener, true);
        assertFalse(result);
        verify(listener).onFailure(argThat(new ExceptionMatcher("Cannot scale to zero without search replicas")));
    }

    public void testValidateScalePrerequisites_ScaleDown_RemoteStoreNotEnabled() {
        // If remote store is not enabled, validation should fail.
        Settings settings = Settings.builder()
            .put(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, false)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, "SEGMENT")
            .build();
        IndexMetadata indexMetadata = createTestIndexMetadata("test-index", settings, 1);
        boolean result = validator.validateScalePrerequisites(indexMetadata, "test-index", listener, true);
        assertFalse(result);
        verify(listener).onFailure(argThat(new ExceptionMatcher(IndexMetadata.SETTING_REMOTE_STORE_ENABLED)));
    }

    public void testValidateScalePrerequisites_ScaleDown_InvalidReplicationType() {
        // If the replication type is not SEGMENT, validation should fail.
        Settings settings = Settings.builder()
            .put(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, "OTHER")
            .build();
        IndexMetadata indexMetadata = createTestIndexMetadata("test-index", settings, 1);
        boolean result = validator.validateScalePrerequisites(indexMetadata, "test-index", listener, true);
        assertFalse(result);
        verify(listener).onFailure(argThat(new ExceptionMatcher("segment replication must be enabled")));
    }

    public void testValidateScalePrerequisites_ScaleDown_Valid() {
        // All prerequisites for scaling down are met.
        Settings settings = Settings.builder()
            .put(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, "SEGMENT")
            .build();
        IndexMetadata indexMetadata = createTestIndexMetadata("test-index", settings, 1);
        boolean result = validator.validateScalePrerequisites(indexMetadata, "test-index", listener, true);
        assertTrue(result);
        verify(listener, never()).onFailure(any());
    }

    public void testValidateScalePrerequisites_ScaleUp_NotSearchOnly() {
        // For scale-up, the index must be in search-only mode.
        Settings settings = Settings.builder().put(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), false).build();
        IndexMetadata indexMetadata = createTestIndexMetadata("test-index", settings, 1);
        boolean result = validator.validateScalePrerequisites(indexMetadata, "test-index", listener, false);
        assertFalse(result);
        verify(listener).onFailure(argThat(new ExceptionMatcher("not in search-only mode")));
    }

    public void testValidateScalePrerequisites_ScaleUp_Valid() {
        // Valid scale-up: the index is in search-only mode.
        Settings settings = Settings.builder().put(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), true).build();
        IndexMetadata indexMetadata = createTestIndexMetadata("test-index", settings, 1);
        boolean result = validator.validateScalePrerequisites(indexMetadata, "test-index", listener, false);
        assertTrue(result);
        verify(listener, never()).onFailure(any());
    }

    /**
     * Helper method to create a dummy IndexMetadata.
     * Adjust this helper to match your actual IndexMetadata builder.
     */
    private IndexMetadata createTestIndexMetadata(String indexName, Settings settings, int searchOnlyReplicas) {
        Settings updatedSettings = Settings.builder()
            .put(settings)
            // Add the required index version setting. You can use a hardcoded value or Version.CURRENT.toString()
            .put("index.version.created", Version.CURRENT)
            .build();
        return IndexMetadata.builder(indexName)
            .settings(updatedSettings)
            .numberOfShards(1)
            .numberOfReplicas(1)
            .numberOfSearchReplicas(searchOnlyReplicas)
            .build();
    }

    /**
     * A custom ArgumentMatcher to check that an exception’s message contains a given substring.
     */
    private static class ExceptionMatcher implements ArgumentMatcher<Exception> {
        private final String substring;

        ExceptionMatcher(String substring) {
            this.substring = substring;
        }

        @Override
        public boolean matches(Exception e) {
            return e != null && e.getMessage() != null && e.getMessage().contains(substring);
        }
    }
}
