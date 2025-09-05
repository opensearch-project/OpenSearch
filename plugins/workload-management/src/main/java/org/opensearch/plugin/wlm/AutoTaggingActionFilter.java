/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.plugin.wlm.rule.attribute_extractor.IndicesExtractor;
import org.opensearch.plugin.wlm.spi.AttributeExtension;
import org.opensearch.rule.InMemoryRuleProcessingService;
import org.opensearch.rule.SecurityAttribute;
import org.opensearch.rule.attribute_extractor.AttributeExtractor;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.WlmMode;
import org.opensearch.wlm.WorkloadGroupTask;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * This class is responsible to evaluate and assign the WORKLOAD_GROUP_ID header in ThreadContext
 */
public class AutoTaggingActionFilter implements ActionFilter {
    private final InMemoryRuleProcessingService ruleProcessingService;
    private final ThreadPool threadPool;
    private final Map<Attribute, AttributeExtension> attributeExtensions;
    private final WlmClusterSettingValuesProvider wlmClusterSettingValuesProvider;

    /**
     * Main constructor
     * @param ruleProcessingService provides access to in memory view of rules
     * @param threadPool to access assign the label
     * @param attributeExtensions
     * @param wlmClusterSettingValuesProvider
     */
    public AutoTaggingActionFilter(
        InMemoryRuleProcessingService ruleProcessingService,
        ThreadPool threadPool,
        Map<Attribute, AttributeExtension> attributeExtensions,
        WlmClusterSettingValuesProvider wlmClusterSettingValuesProvider
    ) {
        this.ruleProcessingService = ruleProcessingService;
        this.threadPool = threadPool;
        this.attributeExtensions = attributeExtensions;
        this.wlmClusterSettingValuesProvider = wlmClusterSettingValuesProvider;
    }

    @Override
    public int order() {
        return Integer.MAX_VALUE;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        final boolean isValidRequest = request instanceof SearchRequest;

        if (!isValidRequest || wlmClusterSettingValuesProvider.getWlmMode() == WlmMode.DISABLED) {
            chain.proceed(task, action, request, listener);
            return;
        }
        List<AttributeExtractor<String>> attributeExtractors = new ArrayList<>();
        attributeExtractors.add(new IndicesExtractor((IndicesRequest) request));
        var principalExtension = attributeExtensions.get(SecurityAttribute.PRINCIPAL);
        if (principalExtension != null) {
            attributeExtractors.add(principalExtension.getAttributeExtractor());
        }
        Optional<String> label = ruleProcessingService.evaluateFeatureValue(attributeExtractors);
        label.ifPresent(s -> threadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, s));
        chain.proceed(task, action, request, listener);
    }
}
