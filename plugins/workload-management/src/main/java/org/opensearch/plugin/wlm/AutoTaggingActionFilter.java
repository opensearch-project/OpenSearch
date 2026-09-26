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
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.plugin.wlm.rule.attribute_extractor.IndicesExtractor;
import org.opensearch.plugin.wlm.spi.AttributeExtractorExtension;
import org.opensearch.rule.InMemoryRuleProcessingService;
import org.opensearch.rule.RuleAttribute;
import org.opensearch.rule.attribute_extractor.AttributeExtractor;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.WlmMode;
import org.opensearch.wlm.WorkloadGroupTask;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.opensearch.plugin.wlm.WorkloadManagementPlugin.PRINCIPAL_ATTRIBUTE_NAME;

/**
 * This class is responsible to evaluate and assign the WORKLOAD_GROUP_ID header in ThreadContext
 */
public class AutoTaggingActionFilter implements ActionFilter {
    private final InMemoryRuleProcessingService ruleProcessingService;
    private final ThreadPool threadPool;
    private final Map<Attribute, AttributeExtractorExtension> attributeExtensions;
    private final WlmClusterSettingValuesProvider wlmClusterSettingValuesProvider;
    private final FeatureType featureType;

    /**
     * Main constructor
     * @param ruleProcessingService provides access to in memory view of rules
     * @param threadPool to access assign the label
     * @param attributeExtensions
     * @param wlmClusterSettingValuesProvider
     * @param featureType
     */
    public AutoTaggingActionFilter(
        InMemoryRuleProcessingService ruleProcessingService,
        ThreadPool threadPool,
        Map<Attribute, AttributeExtractorExtension> attributeExtensions,
        WlmClusterSettingValuesProvider wlmClusterSettingValuesProvider,
        FeatureType featureType
    ) {
        this.ruleProcessingService = ruleProcessingService;
        this.threadPool = threadPool;
        this.attributeExtensions = attributeExtensions;
        this.wlmClusterSettingValuesProvider = wlmClusterSettingValuesProvider;
        this.featureType = featureType;
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
        ActionRequestMetadata<Request, Response> actionRequestMetadata,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        final boolean isSearchRequest = request instanceof SearchRequest;
        final boolean isSearchScrollRequest = request instanceof SearchScrollRequest;
        final boolean isValidRequest = isSearchRequest || isSearchScrollRequest;

        if (!isValidRequest || wlmClusterSettingValuesProvider.getWlmMode() == WlmMode.DISABLED) {
            chain.proceed(task, action, request, listener);
            return;
        }
        List<AttributeExtractor<String>> attributeExtractors = new ArrayList<>();
        if (isSearchRequest) {
            attributeExtractors.add(new IndicesExtractor((IndicesRequest) request));
        } else {
            // Scroll: recover the original user-provided indices from ParsedScrollId
            final String[] originalIndices = ((SearchScrollRequest) request).originalIndicesOrEmpty();
            if (originalIndices.length > 0) {
                attributeExtractors.add(new AttributeExtractor<>() {
                    @Override
                    public Attribute getAttribute() {
                        return RuleAttribute.INDEX_PATTERN;
                    }

                    @Override
                    public Iterable<String> extract() {
                        return Arrays.asList(originalIndices);
                    }

                    @Override
                    public LogicalOperator getLogicalOperator() {
                        return LogicalOperator.AND;
                    }
                });
            }
        }

        List<String> principalValues = null;
        if (featureType.getAllowedAttributesRegistry().containsKey(PRINCIPAL_ATTRIBUTE_NAME)) {
            Attribute attribute = featureType.getAllowedAttributesRegistry().get(PRINCIPAL_ATTRIBUTE_NAME);
            assert attributeExtensions.containsKey(attribute);
            final AttributeExtractor<String> extractor = attributeExtensions.get(attribute).getAttributeExtractor();
            // Materialize once. The value is needed both for label evaluation and for the principal header, and
            // AttributeExtractor.extract() carries no re-iterability contract -- a stream-backed implementation would
            // yield nothing the second time and silently disable username/role throttling.
            final List<String> values = new ArrayList<>();
            extractor.extract().forEach(values::add);
            principalValues = values;
            attributeExtractors.add(new AttributeExtractor<>() {
                @Override
                public Attribute getAttribute() {
                    return extractor.getAttribute();
                }

                @Override
                public Iterable<String> extract() {
                    return values;
                }

                @Override
                public LogicalOperator getLogicalOperator() {
                    return extractor.getLogicalOperator();
                }
            });
        }

        Optional<String> label = ruleProcessingService.evaluateLabel(attributeExtractors);
        label.ifPresent(s -> threadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, s));
        // Hand the principal to core-side throttling so it can build per-username / per-role buckets. It goes on the
        // task, not into the thread context: see WorkloadGroupTask#setThrottlePrincipal.
        if (principalValues != null && task instanceof WorkloadGroupTask) {
            String principal = String.join(WorkloadGroupTask.WORKLOAD_GROUP_PRINCIPAL_VALUE_DELIMITER, principalValues);
            if (principal.isEmpty() == false) {
                ((WorkloadGroupTask) task).setThrottlePrincipal(principal);
            }
        }
        chain.proceed(task, action, request, listener);
    }
}
