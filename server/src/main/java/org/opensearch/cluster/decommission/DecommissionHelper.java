/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.decommission;

import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;

/**
 * Provides a set of static helpers required in decommission flow
 *
 * @opensearch.internal
 */
public class DecommissionHelper {

    public static boolean nodeHasDecommissionedAttribute(DiscoveryNode discoveryNode, DecommissionAttribute decommissionAttribute) {
        return discoveryNode.getAttributes().get(decommissionAttribute.attributeName()).equals(decommissionAttribute.attributeValue());
    }

    public static boolean nodeCommissioned(DiscoveryNode discoveryNode, Metadata metadata) {
        DecommissionAttributeMetadata decommissionAttributeMetadata = metadata.decommissionAttributeMetadata();
        if (decommissionAttributeMetadata != null) {
            DecommissionAttribute decommissionAttribute = decommissionAttributeMetadata.decommissionAttribute();
            DecommissionStatus status = decommissionAttributeMetadata.status();
            if (decommissionAttribute != null && status != null) {
                // We will let the node join the cluster if the current status is in FAILED state
                if (nodeHasDecommissionedAttribute(discoveryNode, decommissionAttribute)
                    && (status.equals(DecommissionStatus.IN_PROGRESS) || status.equals(DecommissionStatus.SUCCESSFUL))) {
                    return false;
                }
            }
        }
        return true;
    }
}
