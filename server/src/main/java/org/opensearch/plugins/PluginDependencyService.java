/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service for tracking plugin dependencies and usage across the cluster
 *
 * @opensearch.internal
 */
public class PluginDependencyService {
    
    private static final Logger logger = LogManager.getLogger(PluginDependencyService.class);
    
    private static final String PLUGIN_USAGE_INDEX = ".opensearch-plugin-usage";
    private static final String PLUGIN_USAGE_TYPE = "_doc";
    
    private final Client client;
    private final ClusterService clusterService;
    private final Settings settings;
    private final PluginsService pluginsService;
    
    // In-memory cache for quick lookups
    private final Map<String, List<PluginUsageDocument>> pluginUsageCache = new ConcurrentHashMap<>();
    
    public PluginDependencyService(
        Client client,
        ClusterService clusterService,
        Settings settings,
        PluginsService pluginsService
    ) {
        this.client = client;
        this.clusterService = clusterService;
        this.settings = settings;
        this.pluginsService = pluginsService;
    }
    
    /**
     * Track analyzer usage in an index mapping
     */
    public void trackAnalyzerUsage(String pluginName, String analyzerName, String indexName, String fieldPath) {
        PluginUsageDocument usage = new PluginUsageDocument()
            .setPluginName(pluginName)
            .setUsageType("analyzer")
            .setResourceName(analyzerName)
            .setIndexName(indexName)
            .setFieldMapping(fieldPath)
            .setTimestamp(Instant.now())
            .setNodeId(clusterService.localNode().getId())
            .setActive(true);
        
        indexPluginUsage(usage);
        updateCache(usage);
    }
    
    /**
     * Track ingest processor usage in a pipeline
     */
    public void trackIngestProcessorUsage(String pluginName, String processorName, String pipelineName) {
        PluginUsageDocument usage = new PluginUsageDocument()
            .setPluginName(pluginName)
            .setUsageType("ingest_processor")
            .setResourceName(processorName)
            .setPipelineName(pipelineName)
            .setTimestamp(Instant.now())
            .setNodeId(clusterService.localNode().getId())
            .setActive(true);
        
        indexPluginUsage(usage);
        updateCache(usage);
    }
    
    /**
     * Track tokenizer usage
     */
    public void trackTokenizerUsage(String pluginName, String tokenizerName, String indexName, String fieldPath) {
        PluginUsageDocument usage = new PluginUsageDocument()
            .setPluginName(pluginName)
            .setUsageType("tokenizer")
            .setResourceName(tokenizerName)
            .setIndexName(indexName)
            .setFieldMapping(fieldPath)
            .setTimestamp(Instant.now())
            .setNodeId(clusterService.localNode().getId())
            .setActive(true);
        
        indexPluginUsage(usage);
        updateCache(usage);
    }
    
    /**
     * Validate if a plugin can be safely reloaded
     */
    public PluginReloadValidation validatePluginReload(String pluginName) {
        PluginReloadValidation validation = new PluginReloadValidation();
        validation.setPluginName(pluginName);
        
        try {
            // Get active usages
            List<PluginUsageDocument> activeUsages = getActivePluginUsages(pluginName);
            validation.setActiveUsages(activeUsages);
            
            // Check dependent plugins
            List<String> dependentPlugins = getDependentPlugins(pluginName);
            validation.setDependentPlugins(dependentPlugins);
            
            // Assess impact
            ReloadImpactAssessment impact = assessReloadImpact(pluginName, activeUsages);
            validation.setImpactAssessment(impact);
            
            // Determine safety
            boolean safeToReload = determineSafety(activeUsages, dependentPlugins, impact);
            validation.setSafeToReload(safeToReload);
            
            // Generate recommendations
            List<String> recommendations = generateRecommendations(activeUsages, impact);
            validation.setRecommendations(recommendations);
            
        } catch (Exception e) {
            logger.error("Failed to validate plugin reload for [{}]", pluginName, e);
            validation.setSafeToReload(false);
            validation.setErrorMessage("Validation failed: " + e.getMessage());
        }
        
        return validation;
    }
    
    /**
     * Get plugin usage statistics
     */
    public PluginUsageStats getPluginUsageStats(String pluginName) {
        List<PluginUsageDocument> usages = getActivePluginUsages(pluginName);
        
        PluginUsageStats stats = new PluginUsageStats();
        stats.setPluginName(pluginName);
        stats.setTotalUsages(usages.size());
        
        Map<String, Integer> usageBreakdown = new HashMap<>();
        List<String> affectedIndices = new ArrayList<>();
        List<String> affectedPipelines = new ArrayList<>();
        
        for (PluginUsageDocument usage : usages) {
            // Count by usage type
            usageBreakdown.merge(usage.getUsageType(), 1, Integer::sum);
            
            // Collect affected resources
            if (usage.getIndexName() != null && !affectedIndices.contains(usage.getIndexName())) {
                affectedIndices.add(usage.getIndexName());
            }
            if (usage.getPipelineName() != null && !affectedPipelines.contains(usage.getPipelineName())) {
                affectedPipelines.add(usage.getPipelineName());
            }
        }
        
        stats.setUsageBreakdown(usageBreakdown);
        stats.setAffectedIndices(affectedIndices);
        stats.setAffectedPipelines(affectedPipelines);
        stats.setLastUpdated(Instant.now());
        
        return stats;
    }
    
    /**
     * Remove usage tracking for a plugin (when unloaded)
     */
    public void removePluginUsage(String pluginName) {
        try {
            // Mark all usages as inactive
            List<PluginUsageDocument> usages = getActivePluginUsages(pluginName);
            for (PluginUsageDocument usage : usages) {
                usage.setActive(false);
                indexPluginUsage(usage);
            }
            
            // Clear from cache
            pluginUsageCache.remove(pluginName);
            
        } catch (Exception e) {
            logger.error("Failed to remove plugin usage for [{}]", pluginName, e);
        }
    }
    
    private void indexPluginUsage(PluginUsageDocument usage) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.field("plugin_name", usage.getPluginName());
            builder.field("plugin_version", getPluginVersion(usage.getPluginName()));
            builder.field("usage_type", usage.getUsageType());
            builder.field("resource_name", usage.getResourceName());
            if (usage.getIndexName() != null) {
                builder.field("index_name", usage.getIndexName());
            }
            if (usage.getFieldMapping() != null) {
                builder.field("field_mapping", usage.getFieldMapping());
            }
            if (usage.getPipelineName() != null) {
                builder.field("pipeline_name", usage.getPipelineName());
            }
            builder.field("timestamp", usage.getTimestamp().toString());
            builder.field("node_id", usage.getNodeId());
            builder.field("active", usage.isActive());
            builder.endObject();
            
            IndexRequest request = new IndexRequest(PLUGIN_USAGE_INDEX)
                .id(generateUsageId(usage))
                .source(builder);
            
            client.index(request);
            
        } catch (IOException e) {
            logger.error("Failed to index plugin usage", e);
        }
    }
    
    private List<PluginUsageDocument> getActivePluginUsages(String pluginName) {
        // Check cache first
        List<PluginUsageDocument> cached = pluginUsageCache.get(pluginName);
        if (cached != null) {
            return cached;
        }
        
        // Query from index
        try {
            SearchRequest searchRequest = new SearchRequest(PLUGIN_USAGE_INDEX);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(
                QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery("plugin_name", pluginName))
                    .must(QueryBuilders.termQuery("active", true))
            );
            searchRequest.source(searchSourceBuilder);
            
            SearchResponse response = client.search(searchRequest).actionGet();
            
            List<PluginUsageDocument> usages = new ArrayList<>();
            response.getHits().forEach(hit -> {
                // Parse hit and create PluginUsageDocument
                // Implementation would parse the source and create objects
            });
            
            // Update cache
            pluginUsageCache.put(pluginName, usages);
            return usages;
            
        } catch (Exception e) {
            logger.error("Failed to get active plugin usages for [{}]", pluginName, e);
            return new ArrayList<>();
        }
    }
    
    private List<String> getDependentPlugins(String pluginName) {
        List<String> dependentPlugins = new ArrayList<>();
        
        // Check all loaded plugins for dependencies on this plugin
        for (PluginInfo pluginInfo : pluginsService.info().getPluginInfos()) {
            if (pluginInfo.getExtendedPlugins().contains(pluginName)) {
                dependentPlugins.add(pluginInfo.getName());
            }
        }
        
        return dependentPlugins;
    }
    
    private ReloadImpactAssessment assessReloadImpact(String pluginName, List<PluginUsageDocument> usages) {
        ReloadImpactAssessment assessment = new ReloadImpactAssessment();
        
        long searchImpactedIndices = usages.stream()
            .filter(u -> "analyzer".equals(u.getUsageType()) || "tokenizer".equals(u.getUsageType()) || "filter".equals(u.getUsageType()))
            .map(PluginUsageDocument::getIndexName)
            .distinct()
            .count();
        
        long ingestionImpactedPipelines = usages.stream()
            .filter(u -> "ingest_processor".equals(u.getUsageType()))
            .map(PluginUsageDocument::getPipelineName)
            .distinct()
            .count();
        
        assessment.setSearchImpactedIndices(searchImpactedIndices);
        assessment.setIngestionImpactedPipelines(ingestionImpactedPipelines);
        assessment.setRiskLevel(calculateRiskLevel(searchImpactedIndices, ingestionImpactedPipelines));
        
        return assessment;
    }
    
    private boolean determineSafety(List<PluginUsageDocument> activeUsages, List<String> dependentPlugins, ReloadImpactAssessment impact) {
        // Plugin is safe to reload if:
        // 1. No active usages
        // 2. No dependent plugins
        // 3. Risk level is LOW or MEDIUM (configurable)
        
        if (!activeUsages.isEmpty()) {
            return false;
        }
        
        if (!dependentPlugins.isEmpty()) {
            return false;
        }
        
        String maxRiskLevel = settings.get("plugins.safe_reload.max_risk_level", "MEDIUM");
        return isRiskLevelAcceptable(impact.getRiskLevel(), maxRiskLevel);
    }
    
    private List<String> generateRecommendations(List<PluginUsageDocument> usages, ReloadImpactAssessment impact) {
        List<String> recommendations = new ArrayList<>();
        
        if (impact.getIngestionImpactedPipelines() > 0) {
            recommendations.add("Stop ingestion to affected pipelines before reload");
        }
        
        if (impact.getSearchImpactedIndices() > 0) {
            recommendations.add("Consider maintenance window for affected indices");
        }
        
        if ("HIGH".equals(impact.getRiskLevel())) {
            recommendations.add("High risk reload - consider rolling restart instead");
        }
        
        return recommendations;
    }
    
    private String calculateRiskLevel(long searchImpacted, long ingestionImpacted) {
        if (searchImpacted == 0 && ingestionImpacted == 0) {
            return "LOW";
        } else if (searchImpacted <= 2 && ingestionImpacted <= 1) {
            return "MEDIUM";
        } else {
            return "HIGH";
        }
    }
    
    private boolean isRiskLevelAcceptable(String currentRisk, String maxAcceptableRisk) {
        int currentLevel = getRiskLevelValue(currentRisk);
        int maxLevel = getRiskLevelValue(maxAcceptableRisk);
        return currentLevel <= maxLevel;
    }
    
    private int getRiskLevelValue(String riskLevel) {
        switch (riskLevel) {
            case "LOW": return 1;
            case "MEDIUM": return 2;
            case "HIGH": return 3;
            default: return 3;
        }
    }
    
    private String getPluginVersion(String pluginName) {
        for (PluginInfo pluginInfo : pluginsService.info().getPluginInfos()) {
            if (pluginInfo.getName().equals(pluginName)) {
                return pluginInfo.getVersion();
            }
        }
        return "unknown";
    }
    
    private String generateUsageId(PluginUsageDocument usage) {
        return String.format("%s-%s-%s-%s", 
            usage.getPluginName(), 
            usage.getUsageType(), 
            usage.getResourceName(),
            usage.getIndexName() != null ? usage.getIndexName() : usage.getPipelineName()
        );
    }
    
    private void updateCache(PluginUsageDocument usage) {
        pluginUsageCache.computeIfAbsent(usage.getPluginName(), k -> new ArrayList<>()).add(usage);
    }
}
