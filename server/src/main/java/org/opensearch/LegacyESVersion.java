/*
 * Licensed to OpenSearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. OpenSearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.opensearch;

public class LegacyESVersion extends Version {

    public static final LegacyESVersion V_6_0_0_alpha1 =
        new LegacyESVersion(6000001, org.apache.lucene.util.Version.LUCENE_7_0_0);
    public static final LegacyESVersion V_6_0_0_alpha2 =
        new LegacyESVersion(6000002, org.apache.lucene.util.Version.LUCENE_7_0_0);
    public static final LegacyESVersion V_6_0_0_beta1 =
        new LegacyESVersion(6000026, org.apache.lucene.util.Version.LUCENE_7_0_0);
    public static final LegacyESVersion V_6_0_0_beta2 =
        new LegacyESVersion(6000027, org.apache.lucene.util.Version.LUCENE_7_0_0);
    public static final LegacyESVersion V_6_0_0_rc1 =
        new LegacyESVersion(6000051, org.apache.lucene.util.Version.LUCENE_7_0_0);
    public static final LegacyESVersion V_6_0_0_rc2 =
        new LegacyESVersion(6000052, org.apache.lucene.util.Version.LUCENE_7_0_1);
    public static final LegacyESVersion V_6_0_0 =
        new LegacyESVersion(6000099, org.apache.lucene.util.Version.LUCENE_7_0_1);
    public static final LegacyESVersion V_6_0_1 =
        new LegacyESVersion(6000199, org.apache.lucene.util.Version.LUCENE_7_0_1);
    public static final LegacyESVersion V_6_1_0 = new LegacyESVersion(6010099, org.apache.lucene.util.Version.LUCENE_7_1_0);
    public static final LegacyESVersion V_6_1_1 = new LegacyESVersion(6010199, org.apache.lucene.util.Version.LUCENE_7_1_0);
    public static final LegacyESVersion V_6_1_2 = new LegacyESVersion(6010299, org.apache.lucene.util.Version.LUCENE_7_1_0);
    public static final LegacyESVersion V_6_1_3 = new LegacyESVersion(6010399, org.apache.lucene.util.Version.LUCENE_7_1_0);
    public static final LegacyESVersion V_6_1_4 = new LegacyESVersion(6010499, org.apache.lucene.util.Version.LUCENE_7_1_0);
    public static final LegacyESVersion V_6_3_0 = new LegacyESVersion(6030099, org.apache.lucene.util.Version.LUCENE_7_3_1);
    public static final LegacyESVersion V_6_3_1 = new LegacyESVersion(6030199, org.apache.lucene.util.Version.LUCENE_7_3_1);
    public static final LegacyESVersion V_6_3_2 = new LegacyESVersion(6030299, org.apache.lucene.util.Version.LUCENE_7_3_1);
    public static final LegacyESVersion V_6_4_0 = new LegacyESVersion(6040099, org.apache.lucene.util.Version.LUCENE_7_4_0);
    public static final LegacyESVersion V_6_4_1 = new LegacyESVersion(6040199, org.apache.lucene.util.Version.LUCENE_7_4_0);
    public static final LegacyESVersion V_6_4_2 = new LegacyESVersion(6040299, org.apache.lucene.util.Version.LUCENE_7_4_0);
    public static final LegacyESVersion V_6_4_3 = new LegacyESVersion(6040399, org.apache.lucene.util.Version.LUCENE_7_4_0);
    public static final LegacyESVersion V_6_5_0 = new LegacyESVersion(6050099, org.apache.lucene.util.Version.LUCENE_7_5_0);
    public static final LegacyESVersion V_6_5_1 = new LegacyESVersion(6050199, org.apache.lucene.util.Version.LUCENE_7_5_0);
    public static final LegacyESVersion V_6_5_2 = new LegacyESVersion(6050299, org.apache.lucene.util.Version.LUCENE_7_5_0);
    public static final LegacyESVersion V_6_5_3 = new LegacyESVersion(6050399, org.apache.lucene.util.Version.LUCENE_7_5_0);
    public static final LegacyESVersion V_6_5_4 = new LegacyESVersion(6050499, org.apache.lucene.util.Version.LUCENE_7_5_0);
    public static final LegacyESVersion V_6_6_0 = new LegacyESVersion(6060099, org.apache.lucene.util.Version.LUCENE_7_6_0);
    public static final LegacyESVersion V_6_6_1 = new LegacyESVersion(6060199, org.apache.lucene.util.Version.LUCENE_7_6_0);
    public static final LegacyESVersion V_6_6_2 = new LegacyESVersion(6060299, org.apache.lucene.util.Version.LUCENE_7_6_0);
    public static final LegacyESVersion V_6_7_0 = new LegacyESVersion(6070099, org.apache.lucene.util.Version.LUCENE_7_7_0);
    public static final LegacyESVersion V_6_7_1 = new LegacyESVersion(6070199, org.apache.lucene.util.Version.LUCENE_7_7_0);
    public static final LegacyESVersion V_6_7_2 = new LegacyESVersion(6070299, org.apache.lucene.util.Version.LUCENE_7_7_0);
    public static final LegacyESVersion V_6_8_0 = new LegacyESVersion(6080099, org.apache.lucene.util.Version.LUCENE_7_7_0);
    public static final LegacyESVersion V_6_8_1 = new LegacyESVersion(6080199, org.apache.lucene.util.Version.LUCENE_7_7_0);
    public static final LegacyESVersion V_6_8_2 = new LegacyESVersion(6080299, org.apache.lucene.util.Version.LUCENE_7_7_0);
    public static final LegacyESVersion V_6_8_3 = new LegacyESVersion(6080399, org.apache.lucene.util.Version.LUCENE_7_7_0);
    public static final LegacyESVersion V_6_8_4 = new LegacyESVersion(6080499, org.apache.lucene.util.Version.LUCENE_7_7_2);
    public static final LegacyESVersion V_6_8_5 = new LegacyESVersion(6080599, org.apache.lucene.util.Version.LUCENE_7_7_2);
    public static final LegacyESVersion V_6_8_6 = new LegacyESVersion(6080699, org.apache.lucene.util.Version.LUCENE_7_7_2);
    public static final LegacyESVersion V_6_8_7 = new LegacyESVersion(6080799, org.apache.lucene.util.Version.LUCENE_7_7_2);
    public static final LegacyESVersion V_6_8_8 = new LegacyESVersion(6080899, org.apache.lucene.util.Version.LUCENE_7_7_2);
    public static final LegacyESVersion V_6_8_15 = new LegacyESVersion(6081599, org.apache.lucene.util.Version.LUCENE_7_7_3);
    public static final LegacyESVersion V_7_0_0 = new LegacyESVersion(7000099, org.apache.lucene.util.Version.LUCENE_8_0_0);
    public static final LegacyESVersion V_7_0_1 = new LegacyESVersion(7000199, org.apache.lucene.util.Version.LUCENE_8_0_0);
    public static final LegacyESVersion V_7_1_0 = new LegacyESVersion(7010099, org.apache.lucene.util.Version.LUCENE_8_0_0);
    public static final LegacyESVersion V_7_1_1 = new LegacyESVersion(7010199, org.apache.lucene.util.Version.LUCENE_8_0_0);
    public static final LegacyESVersion V_7_2_0 = new LegacyESVersion(7020099, org.apache.lucene.util.Version.LUCENE_8_0_0);
    public static final LegacyESVersion V_7_2_1 = new LegacyESVersion(7020199, org.apache.lucene.util.Version.LUCENE_8_0_0);
    public static final LegacyESVersion V_7_3_0 = new LegacyESVersion(7030099, org.apache.lucene.util.Version.LUCENE_8_1_0);
    public static final LegacyESVersion V_7_3_1 = new LegacyESVersion(7030199, org.apache.lucene.util.Version.LUCENE_8_1_0);
    public static final LegacyESVersion V_7_3_2 = new LegacyESVersion(7030299, org.apache.lucene.util.Version.LUCENE_8_1_0);
    public static final LegacyESVersion V_7_4_0 = new LegacyESVersion(7040099, org.apache.lucene.util.Version.LUCENE_8_2_0);
    public static final LegacyESVersion V_7_4_1 = new LegacyESVersion(7040199, org.apache.lucene.util.Version.LUCENE_8_2_0);
    public static final LegacyESVersion V_7_4_2 = new LegacyESVersion(7040299, org.apache.lucene.util.Version.LUCENE_8_2_0);
    public static final LegacyESVersion V_7_5_0 = new LegacyESVersion(7050099, org.apache.lucene.util.Version.LUCENE_8_3_0);
    public static final LegacyESVersion V_7_5_1 = new LegacyESVersion(7050199, org.apache.lucene.util.Version.LUCENE_8_3_0);
    public static final LegacyESVersion V_7_5_2 = new LegacyESVersion(7050299, org.apache.lucene.util.Version.LUCENE_8_3_0);
    public static final LegacyESVersion V_7_6_0 = new LegacyESVersion(7060099, org.apache.lucene.util.Version.LUCENE_8_4_0);
    public static final LegacyESVersion V_7_6_1 = new LegacyESVersion(7060199, org.apache.lucene.util.Version.LUCENE_8_4_0);
    public static final LegacyESVersion V_7_6_2 = new LegacyESVersion(7060299, org.apache.lucene.util.Version.LUCENE_8_4_0);
    public static final LegacyESVersion V_7_7_0 = new LegacyESVersion(7070099, org.apache.lucene.util.Version.LUCENE_8_5_1);
    public static final LegacyESVersion V_7_7_1 = new LegacyESVersion(7070199, org.apache.lucene.util.Version.LUCENE_8_5_1);
    public static final LegacyESVersion V_7_8_0 = new LegacyESVersion(7080099, org.apache.lucene.util.Version.LUCENE_8_5_1);
    public static final LegacyESVersion V_7_8_1 = new LegacyESVersion(7080199, org.apache.lucene.util.Version.LUCENE_8_5_1);
    public static final LegacyESVersion V_7_9_0 = new LegacyESVersion(7090099, org.apache.lucene.util.Version.LUCENE_8_6_0);
    public static final LegacyESVersion V_7_9_1 = new LegacyESVersion(7090199, org.apache.lucene.util.Version.LUCENE_8_6_2);
    public static final LegacyESVersion V_7_9_2 = new LegacyESVersion(7090299, org.apache.lucene.util.Version.LUCENE_8_6_2);
    public static final LegacyESVersion V_7_9_3 = new LegacyESVersion(7090399, org.apache.lucene.util.Version.LUCENE_8_6_2);
    public static final LegacyESVersion V_7_10_0 = new LegacyESVersion(7100099, org.apache.lucene.util.Version.LUCENE_8_7_0);
    public static final LegacyESVersion V_7_10_1 = new LegacyESVersion(7100199, org.apache.lucene.util.Version.LUCENE_8_7_0);
    public static final LegacyESVersion V_7_10_2 = new LegacyESVersion(7100299, org.apache.lucene.util.Version.LUCENE_8_7_0);
    public static final LegacyESVersion V_7_10_3 = new LegacyESVersion(7100399, org.apache.lucene.util.Version.LUCENE_8_7_0);
    // The below version is missing from the 7.3 JAR
    private static final org.apache.lucene.util.Version LUCENE_7_2_1 = org.apache.lucene.util.Version.fromBits(7, 2, 1);
    public static final LegacyESVersion V_6_2_4 = new LegacyESVersion(6020499, LUCENE_7_2_1);
    public static final LegacyESVersion V_6_2_3 = new LegacyESVersion(6020399, LUCENE_7_2_1);
    public static final LegacyESVersion V_6_2_2 = new LegacyESVersion(6020299, LUCENE_7_2_1);
    public static final LegacyESVersion V_6_2_1 = new LegacyESVersion(6020199, LUCENE_7_2_1);
    public static final LegacyESVersion V_6_2_0 = new LegacyESVersion(6020099, LUCENE_7_2_1);
    // Version constant for Lucene 7.7.3 release with index corruption bug fix
    private static final org.apache.lucene.util.Version LUCENE_7_7_3 = org.apache.lucene.util.Version.fromBits(7, 7, 3);
    public static final LegacyESVersion V_6_8_14 = new LegacyESVersion(6081499, LUCENE_7_7_3);
    public static final LegacyESVersion V_6_8_13 = new LegacyESVersion(6081399, LUCENE_7_7_3);
    public static final LegacyESVersion V_6_8_12 = new LegacyESVersion(6081299, LUCENE_7_7_3);
    public static final LegacyESVersion V_6_8_11 = new LegacyESVersion(6081199, LUCENE_7_7_3);
    public static final LegacyESVersion V_6_8_10 = new LegacyESVersion(6081099, LUCENE_7_7_3);
    public static final LegacyESVersion V_6_8_9 = new LegacyESVersion(6080999, LUCENE_7_7_3);

    protected LegacyESVersion(int id, org.apache.lucene.util.Version luceneVersion) {
        // flip the 28th bit of the version id
        // this will be flipped back in the parent class to correctly identify as a legacy version;
        // giving backwards compatibility with legacy systems
        super(id ^ 0x08000000, luceneVersion);
    }
}
