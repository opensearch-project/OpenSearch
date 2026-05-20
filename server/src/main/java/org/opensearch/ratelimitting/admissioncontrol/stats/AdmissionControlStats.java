/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Class for admission control stats used as part of node stats
 * @opensearch.internal
 */
public class AdmissionControlStats implements ToXContentFragment, Writeable {

    private final List<AdmissionControllerStats> admissionControllerStatsList;

    /**
     *
     * @param admissionControllerStatsList list of admissionControllerStats
     */
    public AdmissionControlStats(List<AdmissionControllerStats> admissionControllerStatsList) {
        this.admissionControllerStatsList = admissionControllerStatsList;
    }

    /**
     *
     * @param in the stream to read from
     * @throws IOException if an I/O error occurs
     */
    public AdmissionControlStats(StreamInput in) throws IOException {
        this.admissionControllerStatsList = in.readList(AdmissionControllerStats::new);
    }

    /**
     * Write this into the {@linkplain StreamOutput}.
     *
     * @param out the output stream to write entity content to
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(this.admissionControllerStatsList);
    }

    public List<AdmissionControllerStats> getAdmissionControllerStatsList() {
        return admissionControllerStatsList;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("admission_control");
        for (AdmissionControllerStats admissionControllerStats : this.admissionControllerStatsList) {
            builder.field(admissionControllerStats.getAdmissionControllerName(), admissionControllerStats);
        }
        return builder.endObject();
    }
}
