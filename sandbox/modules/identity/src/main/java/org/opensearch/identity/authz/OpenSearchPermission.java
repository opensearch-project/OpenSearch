/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.authz;

import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.WildcardPermission;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.Glob;
import org.opensearch.common.regex.Regex;
import org.opensearch.identity.IdentityPlugin;

import java.util.Arrays;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.Collections;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OpenSearchPermission implements Permission {

    private List<String> resourcePatterns;
    private final String PERMISSION_DELIMITER = "\\.";

    public final String RESOURCE_DELIMITER = ",";

    private String permissionString;
    private String[] permissionSegments;

    private String action;
    private String permissionType;

    public OpenSearchPermission(String permission) {

        this.permissionString = permission;
        try {
            this.permissionSegments = permissionString.split(PERMISSION_DELIMITER);
            this.permissionType = permissionSegments[0];
            this.action = permissionSegments[1];
        } catch (IndexOutOfBoundsException ex) {
            throw new PermissionFactory.InvalidPermissionException(
                "All permissions must contain a permission type and" + " action delimited by a " + PERMISSION_DELIMITER + "."
            );
        }
        // Handle two legacy index permissions that are really cluster permissions
        if (this.action.equalsIgnoreCase("admin/template/") || this.action.equalsIgnoreCase("admin/index_template/")) {
            this.resourcePatterns = Arrays.asList("*");
        }
        if (this.permissionSegments.length == 3) {
            String resourceString = permissionSegments[2];
            this.resourcePatterns = Arrays.asList(resourceString.split(RESOURCE_DELIMITER));

        }
    }

    public String getPermissionType() {
        return this.permissionType;
    }

    public String getAction() {
        return this.action;
    }

    public List<String> getResource() {
        return this.resourcePatterns;
    }

    public String getPermissionString() {
        return this.permissionString;
    }

    /**
     * Compare the current permission's permission type to another permission's permission type
     */
    public boolean permissionTypesMatch(OpenSearchPermission permission) {

        if (this.permissionType.equalsIgnoreCase(permission.permissionType)) {
            return true;
        }
        return false;
    }

    /**
     * Resolves complex resource patterns with '-' signs to the corresponding resource patterns
     *
     * @param availableResources list of all resources
     * @param selectedResources  provided resource patterns
     * @return resolved resource patterns
     */
    public static List<String> resolveResourceNegation(List<String> availableResources, List<String> selectedResources) {

        // Move the exclusions to end of list to ensure they are processed
        // after explicitly selected indices are chosen.
        final List<String> excludesAtEndSelectedResources = Stream.concat(
            selectedResources.stream().filter(s -> s.isEmpty() || s.charAt(0) != '-'),
            selectedResources.stream().filter(s -> !s.isEmpty() && s.charAt(0) == '-')
        ).collect(Collectors.toUnmodifiableList());

        Set<String> result = null;
        for (int i = 0; i < excludesAtEndSelectedResources.size(); i++) {
            String resourceOrPattern = excludesAtEndSelectedResources.get(i);
            boolean add = true;
            if (!resourceOrPattern.isEmpty()) {
                if (availableResources.contains(resourceOrPattern)) {
                    if (result == null) {
                        result = new HashSet<>();
                    }
                    result.add(resourceOrPattern);
                    continue;
                }
                if (resourceOrPattern.charAt(0) == '+') {
                    add = true;
                    resourceOrPattern = resourceOrPattern.substring(1);
                    // if its the first, add empty set
                    if (i == 0) {
                        result = new HashSet<>();
                    }
                } else if (resourceOrPattern.charAt(0) == '-') {
                    // If the first resource pattern is an exclusion, then all patterns are exclusions due to the
                    // reordering logic above. In this case, the request is interpreted as "include all resources except
                    // those matching the exclusions" so we add all resources here and then remove the ones that match the exclusion
                    // patterns.
                    if (i == 0) {
                        result = new HashSet<>(availableResources);
                    }
                    add = false;
                    resourceOrPattern = resourceOrPattern.substring(1);
                }
            }
            if (resourceOrPattern.isEmpty() || !Regex.isSimpleMatchPattern(resourceOrPattern)) {
                if (!availableResources.contains(resourceOrPattern)) {
                    throw new ResourceNotFoundException(resourceOrPattern);
                } else {
                    if (result != null) {
                        if (add) {
                            result.add(resourceOrPattern);
                        } else {
                            result.remove(resourceOrPattern);
                        }
                    }
                }
                continue;
            }
            if (result == null) {
                // add all the previous ones...
                result = new HashSet<>(availableResources.subList(0, i));
            }
            boolean found = false;
            for (String resource : availableResources) {
                if (Regex.simpleMatch(resourceOrPattern, resource)) {
                    found = true;
                    if (add) {
                        result.add(resource);
                    } else {
                        result.remove(resource);
                    }
                }
            }
            if (!found) {
                throw new ResourceNotFoundException(resourceOrPattern);
            }
        }
        if (result == null) {
            return Collections.unmodifiableList((selectedResources));
        }
        return Collections.unmodifiableList(new ArrayList<>(result));
    }

    @Override
    public boolean implies(Permission p) {
        if (!(p instanceof OpenSearchPermission)) {
            return false;
        }
        OpenSearchPermission requestedPermission = (OpenSearchPermission) p;

        // Check if permission types match
        if (!permissionTypesMatch(requestedPermission)) {
            return false;
        }

        // Check if permission actions match
        WildcardPermission wp = new WildcardPermission(this.action);
        WildcardPermission wp2 = new WildcardPermission(requestedPermission.action);
        if (!wp.implies(wp2)) {
            return false;
        }

        // Check if resource pattern is empty and resolve
        if (this.resourcePatterns == null || this.resourcePatterns.isEmpty()) {

            // If the matching permission type requires a resource pattern
            if (PermissionFactory.QUALIFIED_PERMISSION_TYPES.matchingType(this.permissionType).isResourcePatternRequired()) {

                return false;
            } else {
                return true;
            }
        }

        // Resolve negated resources
        if (this.resourcePatterns.contains("-")) {
            IndexNameExpressionResolver iner = IndexNameExpressionResolverHolder.getInstance();
            ClusterState cs = IdentityPlugin.GuiceHolder.getClusterService().state();
            // Get all index names that could be associated with a permission
            // TODO: Add plugin and extension names
            Set<String> allPermissionIndexNames = iner.resolveExpressions(cs, "*");
            List<String> allResources = new ArrayList<>(allPermissionIndexNames);
            this.resourcePatterns = resolveResourceNegation(allResources, this.resourcePatterns);
        }

        IndexNameExpressionResolver iner = IndexNameExpressionResolverHolder.getInstance();
        ClusterState cs = IdentityPlugin.GuiceHolder.getClusterService().state();
        Set<String> concretePermissionIndexNames = iner.resolveExpressions(cs, this.resourcePatterns.toArray(new String[0]));
        Set<String> concreteRequestedIndexNames = iner.resolveExpressions(cs, requestedPermission.resourcePatterns.toArray(new String[0]));

        // TODO Switch this comments to tests to better document and assert different scenarios
        // Example
        // User granted index patterns: logs-*,my-index*
        // Request permission on: logs-2023-01-27
        //
        // For all requested index patterns (ip):
        // ip must match at least one pattern that the user has been granted access to

        boolean allRequestedPatternsMatchGranted = requestedPermission.resourcePatterns.stream()
            .allMatch(requstedIp -> this.resourcePatterns.stream().anyMatch(ip -> Glob.globMatch(ip, requstedIp)));

        return allRequestedPatternsMatchGranted;
    }
}
