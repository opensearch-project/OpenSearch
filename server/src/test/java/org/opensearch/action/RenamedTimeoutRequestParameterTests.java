/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action;

import org.junit.After;
import org.opensearch.OpenSearchParseException;
import org.opensearch.action.support.master.MasterNodeRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.action.admin.cluster.RestCleanupRepositoryAction;
import org.opensearch.rest.action.admin.cluster.RestCloneSnapshotAction;
import org.opensearch.rest.action.admin.cluster.RestCreateSnapshotAction;
import org.opensearch.rest.action.admin.cluster.RestDeleteRepositoryAction;
import org.opensearch.rest.action.admin.cluster.RestDeleteSnapshotAction;
import org.opensearch.rest.action.admin.cluster.RestGetRepositoriesAction;
import org.opensearch.rest.action.admin.cluster.RestGetSnapshotsAction;
import org.opensearch.rest.action.admin.cluster.RestPutRepositoryAction;
import org.opensearch.rest.action.admin.cluster.RestRestoreSnapshotAction;
import org.opensearch.rest.action.admin.cluster.RestSnapshotsStatusAction;
import org.opensearch.rest.action.admin.cluster.RestVerifyRepositoryAction;
import org.opensearch.rest.action.cat.RestAllocationAction;
import org.opensearch.rest.action.cat.RestRepositoriesAction;
import org.opensearch.rest.action.cat.RestThreadPoolAction;
import org.opensearch.rest.action.cat.RestMasterAction;
import org.opensearch.rest.action.cat.RestShardsAction;
import org.opensearch.rest.action.cat.RestPluginsAction;
import org.opensearch.rest.action.cat.RestNodeAttrsAction;
import org.opensearch.rest.action.cat.RestNodesAction;
import org.opensearch.rest.action.cat.RestIndicesAction;
import org.opensearch.rest.action.cat.RestTemplatesAction;
import org.opensearch.rest.action.cat.RestPendingClusterTasksAction;
import org.opensearch.rest.action.cat.RestSegmentsAction;
import org.opensearch.rest.action.cat.RestSnapshotAction;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.threadpool.TestThreadPool;

import java.util.Collections;

import static org.hamcrest.Matchers.containsString;

/**
 * As of 2.0, the request parameter 'master_timeout' in all applicable REST APIs is deprecated,
 * and alternative parameter 'cluster_manager_timeout' is added.
 * The tests are used to validate the behavior about the renamed request parameter.
 * Remove the test after removing MASTER_ROLE and 'master_timeout'.
 */
public class RenamedTimeoutRequestParameterTests extends OpenSearchTestCase {
    private final TestThreadPool threadPool = new TestThreadPool(RenamedTimeoutRequestParameterTests.class.getName());
    private final NodeClient client = new NodeClient(Settings.EMPTY, threadPool);
    private final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RenamedTimeoutRequestParameterTests.class);

    private static final String DUPLICATE_PARAMETER_ERROR_MESSAGE =
        "Please only use one of the request parameters [master_timeout, cluster_manager_timeout].";
    private static final String MASTER_TIMEOUT_DEPRECATED_MESSAGE =
        "Deprecated parameter [master_timeout] used. To promote inclusive language, please use [cluster_manager_timeout] instead. It will be unsupported in a future major version.";

    @After
    public void terminateThreadPool() {
        terminate(threadPool);
    }

    public void testNoWarningsForNewParam() {
        BaseRestHandler.parseDeprecatedMasterTimeoutParameter(
            getMasterNodeRequest(),
            getRestRequestWithNewParam(),
            deprecationLogger,
            "test"
        );
    }

    public void testDeprecationWarningForOldParam() {
        BaseRestHandler.parseDeprecatedMasterTimeoutParameter(
            getMasterNodeRequest(),
            getRestRequestWithDeprecatedParam(),
            deprecationLogger,
            "test"
        );
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testBothParamsNotValid() {
        Exception e = assertThrows(
            OpenSearchParseException.class,
            () -> BaseRestHandler.parseDeprecatedMasterTimeoutParameter(
                getMasterNodeRequest(),
                getRestRequestWithBothParams(),
                deprecationLogger,
                "test"
            )
        );
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCatAllocation() {
        RestAllocationAction action = new RestAllocationAction();
        Exception e = assertThrows(OpenSearchParseException.class, () -> action.doCatRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCatIndices() {
        RestIndicesAction action = new RestIndicesAction();
        Exception e = assertThrows(OpenSearchParseException.class, () -> action.doCatRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCatClusterManager() {
        RestMasterAction action = new RestMasterAction();
        Exception e = assertThrows(OpenSearchParseException.class, () -> action.doCatRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCatNodeattrs() {
        RestNodeAttrsAction action = new RestNodeAttrsAction();
        Exception e = assertThrows(OpenSearchParseException.class, () -> action.doCatRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCatNodes() {
        RestNodesAction action = new RestNodesAction();
        Exception e = assertThrows(OpenSearchParseException.class, () -> action.doCatRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCatPendingTasks() {
        RestPendingClusterTasksAction action = new RestPendingClusterTasksAction();
        Exception e = assertThrows(OpenSearchParseException.class, () -> action.doCatRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCatPlugins() {
        RestPluginsAction action = new RestPluginsAction();
        Exception e = assertThrows(OpenSearchParseException.class, () -> action.doCatRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCatRepositories() {
        RestRepositoriesAction action = new RestRepositoriesAction();
        Exception e = assertThrows(OpenSearchParseException.class, () -> action.doCatRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCatShards() {
        RestShardsAction action = new RestShardsAction();
        Exception e = assertThrows(OpenSearchParseException.class, () -> action.doCatRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCatSnapshots() {
        RestSnapshotAction action = new RestSnapshotAction();
        Exception e = assertThrows(OpenSearchParseException.class, () -> action.doCatRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCatTemplates() {
        RestTemplatesAction action = new RestTemplatesAction();
        Exception e = assertThrows(OpenSearchParseException.class, () -> action.doCatRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCatThreadPool() {
        RestThreadPoolAction action = new RestThreadPoolAction();
        Exception e = assertThrows(OpenSearchParseException.class, () -> action.doCatRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCatSegments() {
        RestSegmentsAction action = new RestSegmentsAction();
        Exception e = assertThrows(OpenSearchParseException.class, () -> action.doCatRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCleanupRepository() {
        RestCleanupRepositoryAction action = new RestCleanupRepositoryAction();
        Exception e = assertThrows(OpenSearchParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCloneSnapshot() {
        RestCloneSnapshotAction action = new RestCloneSnapshotAction();
        Exception e = assertThrows(
            OpenSearchParseException.class,
            () -> action.prepareRequest(getRestRequestWithBodyWithBothParams(), client)
        );
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCreateSnapshot() {
        RestCreateSnapshotAction action = new RestCreateSnapshotAction();
        Exception e = assertThrows(OpenSearchParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testDeleteRepository() {
        RestDeleteRepositoryAction action = new RestDeleteRepositoryAction();
        Exception e = assertThrows(OpenSearchParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testDeleteSnapshot() {
        RestDeleteSnapshotAction action = new RestDeleteSnapshotAction();
        Exception e = assertThrows(OpenSearchParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testGetRepositories() {
        final SettingsFilter filter = new SettingsFilter(Collections.singleton("foo.filtered"));
        RestGetRepositoriesAction action = new RestGetRepositoriesAction(filter);
        Exception e = assertThrows(OpenSearchParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testGetSnapshots() {
        RestGetSnapshotsAction action = new RestGetSnapshotsAction();
        Exception e = assertThrows(OpenSearchParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testPutRepository() {
        RestPutRepositoryAction action = new RestPutRepositoryAction();
        Exception e = assertThrows(
            OpenSearchParseException.class,
            () -> action.prepareRequest(getRestRequestWithBodyWithBothParams(), client)
        );
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testRestoreSnapshot() {
        RestRestoreSnapshotAction action = new RestRestoreSnapshotAction();
        Exception e = assertThrows(OpenSearchParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testSnapshotsStatus() {
        RestSnapshotsStatusAction action = new RestSnapshotsStatusAction();
        Exception e = assertThrows(OpenSearchParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testVerifyRepository() {
        RestVerifyRepositoryAction action = new RestVerifyRepositoryAction();
        Exception e = assertThrows(OpenSearchParseException.class, () -> action.prepareRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    private MasterNodeRequest getMasterNodeRequest() {
        return new MasterNodeRequest() {
            @Override
            public ActionRequestValidationException validate() {
                return null;
            }
        };
    }

    private FakeRestRequest getRestRequestWithBothParams() {
        FakeRestRequest request = new FakeRestRequest();
        request.params().put("cluster_manager_timeout", "1h");
        request.params().put("master_timeout", "3s");
        return request;
    }

    private FakeRestRequest getRestRequestWithDeprecatedParam() {
        FakeRestRequest request = new FakeRestRequest();
        request.params().put("master_timeout", "3s");
        return request;
    }

    private FakeRestRequest getRestRequestWithNewParam() {
        FakeRestRequest request = new FakeRestRequest();
        request.params().put("cluster_manager_timeout", "2m");
        return request;
    }

    private FakeRestRequest getRestRequestWithBodyWithBothParams() {
        FakeRestRequest request = getFakeRestRequestWithBody();
        request.params().put("cluster_manager_timeout", "2m");
        request.params().put("master_timeout", "3s");
        return request;
    }

    private FakeRestRequest getFakeRestRequestWithBody() {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(new BytesArray("{}"), XContentType.JSON).build();
    }
}
