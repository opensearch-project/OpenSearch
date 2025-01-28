/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.service;

import org.mockito.ArgumentCaptor;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.wlm.QueryGroupTestUtils;
import org.opensearch.plugin.wlm.rule.action.CreateRuleResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.MutableQueryGroupFragment;
import org.opensearch.wlm.MutableQueryGroupFragment.ResiliencyMode;
import org.opensearch.wlm.ResourceType;

import java.util.*;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.opensearch.cluster.metadata.QueryGroup.builder;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.*;
import static org.opensearch.plugin.wlm.querygroup.action.QueryGroupActionTestUtils.updateQueryGroupRequest;

public class QueryGroupPersistenceServiceTests extends OpenSearchTestCase {

}
