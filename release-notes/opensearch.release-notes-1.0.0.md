## Version 1.0.0 Release Notes

* __Add Snapshot maven repository (#942)__

    [Marc Handalian](mailto:handalm@amazon.com) - Wed, 7 Jul 2021 15:52:30 -0700

    efs/remotes/upstream/1.0, refs/heads/1.0
    * Backport #829 to 1.0 so that we can publish patch snapshots to our maven
    repository
    Signed-off-by: Marc Handalian &lt;handalm@amazon.com&gt;

* __[BUG] Fix cat.health test failures in pre 1.0.0 mixed cluster test (#928) (#935)__

    [Nick Knize](mailto:nknize@gmail.com) - Fri, 2 Jul 2021 17:56:39 -0500

    Fixes the cat.health yaml failures when running in a bwc mixed cluster with

    legacy (pre 1.0.0) nodes.
     Signed-off-by: Nicholas Walter Knize &lt;nknize@apache.org&gt;

* __Update DistributionDownloader to support fetching arm64 bundles. (#929) (#934)__

    [Marc Handalian](mailto:handalm@amazon.com) - Fri, 2 Jul 2021 22:10:24 +0000

    Signed-off-by: Marc Handalian &lt;handalm@amazon.com&gt;

* __Update favicon for OpenSearch (#933)__

    [Rabi Panda](mailto:pandarab@amazon.com) - Fri, 2 Jul 2021 21:43:22 +0000

    Signed-off-by: Rabi Panda &lt;pandarab@amazon.com&gt;

* __Refresh OpenSearch nodes version in cluster state after upgrade (#865) (#926)__

    [Tianli Feng](mailto:ftianli@amazon.com) - Fri, 2 Jul 2021 16:53:47 +0000

    Signed-off-by: Shweta Thareja &lt;tharejas@amazon.com&gt;
    Co-authored-by:
    shwetathareja &lt;shwetathareja@live.com&gt;

* __Fix arm architecture translation issue (#809) (#917)__

    [Peter Nied](mailto:petern@amazon.com) - Fri, 2 Jul 2021 10:58:26 -0500

    Found when attempting to build on an `arm64` machine when I recieved an error
    message below.  Root cause is that string equality in java cannot be done with
    the `==` sign.

    ```
    unknown architecture [arm64] for jdk [provisioned_runtime], must be one of
    [aarch64, x64]
    ```
    Signed-off-by: Peter Nied &lt;petern@amazon.com&gt;

* __Fix Snapshot pattern in DistributionDownloader. (#916) (#920)__

    [Marc Handalian](mailto:handalm@amazon.com) - Thu, 1 Jul 2021 22:01:13 -0500


    Snapshots are published with a -latest flag into s3, this updates
    the pattern
    to correctly point to -latest.
    Signed-off-by: Marc Handalian &lt;handalm@amazon.com&gt;

* __Add &#39;tagline&#39; back to MainResponse in server that was removed in PR #427 (#913) (#915)__

    [Tianli Feng](mailto:ftianli@amazon.com) - Thu, 1 Jul 2021 19:46:15 -0700


    * Add &#34;tagline&#34; field back to &#34;MainResponse&#34; in sever side (not in
    rest-high-level-client side) that removed in PR #427 .
    * Replace with a new tagline &#34;The OpenSearch Project: https://opensearch.org/&#34;.

    * Turn the tagline into a constant in
    server/src/main/java/org/opensearch/action/main/MainResponse.java.

    Signed-off-by: Tianli Feng &lt;ftianli@amazon.com&gt;

* __Remove distribution from main response in compatibility mode (#911)__

    [Marc Handalian](mailto:handalm@amazon.com) - Wed, 30 Jun 2021 19:15:22 -0700

    This Change removes version.distribution when the version.number is
    overridden
    with the cluster setting compatibility.override_main_response_version.
    Backport of https://github.com/opensearch-project/OpenSearch/pull/898

    Signed-off-by: Marc Handalian &lt;handalm@amazon.com&gt;

* __Replace metadata keys in OpenSearchException during serialization and deserialization (#905) (#909)__

    [Vacha](mailto:shahvacha19@gmail.com) - Wed, 30 Jun 2021 14:38:02 -0700

    Signed-off-by: Vlad Rozov &lt;vrozov@users.noreply.github.com&gt;
    Co-authored-by: Vlad Rozov &lt;vrozov@users.noreply.github.com&gt;

* __Update DistributionDownloader to fetch snapshots and staging bundles. (#904) (#906)__

    [Marc Handalian](mailto:handalm@amazon.com) - Wed, 30 Jun 2021 11:32:40 -0700


    This change updates DistributionDownloader by registering a second
    repository
    containing release-candidate bundles.  This repository will
    only be checked if
    the release repository does not contain the requested
    version.
    This change also updates the snapshot repository to point to a new
    snapshot
    repository.
    Signed-off-by: Marc Handalian &lt;handalm@amazon.com&gt;

* __update external library &#39;pdfbox&#39; version to 2.0.24 to reduce vulnerability (#883) (#903)__

    [Tianli Feng](mailto:ftianli@amazon.com) - Tue, 29 Jun 2021 19:30:26 -0700

    Signed-off-by: Tianli Feng &lt;ftianli@amazon.com&gt;


* __Support running elasticsearch-oss distribution in test cluster for BWC (#764) (#774) (#882)__

    [Sarat Vemulapalli](mailto:vemulapallisarat@gmail.com) - Fri, 25 Jun 2021 16:18:26 -0700


    We need to install elasticsearch in the test cluster nodes to be able to run
    the BWC tests. This commit makes the following changes to enable that,
    - updates DistributionDownloadPlugin to download elasticsearch-oss
    distributions.
    - updates OpenSearchNode to be able to run both OpenSearch and Elasticsearch
    distributions.
     Signed-off-by: Rabi Panda &lt;adnapibar@gmail.com&gt;
     Co-authored-by: Rabi Panda &lt;adnapibar@gmail.com&gt;

* __[BUG] Fix versioning issues discovered through version bump (#884) (#887)__

    [Nick Knize](mailto:nknize@gmail.com) - Fri, 25 Jun 2021 17:10:35 -0500


    This commit fixes up the version framework to ensure a smooth version bump
    across releases.
    Signed-off-by: Nicholas Walter Knize &lt;nknize@apache.org&gt;

* __Apply fix for health API response to distinguish no master (#819)__

    [Mohit Godwani](mailto:81609427+mgodwan@users.noreply.github.com) - Fri, 25 Jun 2021 11:05:13 -0500


    Signed-off-by: Mohit Godwani &lt;mgodwan@amazon.com&gt;

* __Enable BWC checks (#796) (#811)__

    [Rabi Panda](mailto:adnapibar@gmail.com) - Fri, 25 Jun 2021 11:04:50 -0500


    Signed-off-by: Rabi Panda &lt;adnapibar@gmail.com&gt;

* __Version checks are incorrectly returning versions &lt; 1.0.0. (#797) (#807)__

    [Daniel Doubrovkine (dB.)](mailto:dblock@dblock.org) - Fri, 25 Jun 2021 11:03:40 -0500


    * Version checks are incorrectly returning versions &lt; 1.0.0.
    * Removed V_7_10_3 which has not been released as of time of the fork.
    * Update check for current version to get unreleased versions.

    - no unreleased version if the current version is &#34;1.0.0&#34;
    - add unit tests for OpenSearch 1.0.0 with legacy ES versions.
    - update VersionUtils to include all legacy ES versions as released.
     Signed-off-by: Rabi Panda &lt;adnapibar@gmail.com&gt;
    Signed-off-by: dblock &lt;dblock@amazon.com&gt;
    Co-authored-by: Rabi Panda &lt;adnapibar@gmail.com&gt;

* __Add cluster setting to spoof version number returned from MainResponse (#847) (#870)__

    [Marc Handalian](mailto:handalm@amazon.com) - Tue, 22 Jun 2021 17:02:31 -0500


    This change adds a new cluster setting
    &#34;compatibility.override_main_response_version&#34;
    that when enabled spoofs the
    version.number returned from MainResponse
    for REST clients expecting legacy
    version 7.10.2.
    Signed-off-by: Marc Handalian &lt;handalm@amazon.com&gt;

* __Fix resource leak issues suggested by Amazon CodeGuru (#816) (#860)__

    [Tianli Feng](mailto:ftianli@amazon.com) - Tue, 22 Jun 2021 10:19:37 -0700


    * Add try-with-resources block to automatically close the resources after using
    to avoid resource leak, in `SymbolicLinkPreservingTarIT`, `LicenseAnalyzer`,
    `SymbolicLinkPreservingUntarTransform`, `ConcurrentSeqNoVersioningIT` in
    `VersionProperties`, `GeoFilterIT`, `XContentHelper`,  `Json` and `IndexShard`
    class

    * Add try-finally block to close the resources after using to avoid resource
    leak, in `ServerChannelContext` class.

    * Add try-catch block to close the resources when exception occurs in
    `FsBlobContainer` class (when XContentFactory.xContentType throws an
    exception).

    * Close resources when assertion error occurs, in `ServerChannelContext` class.

    Signed-off-by: Tianli Feng &lt;ftianli@amazon.com&gt;

* __[BWC] fix mixedCluster and rolling upgrades (#775) (#793)__

    [Nick Knize](mailto:nknize@gmail.com) - Fri, 28 May 2021 10:08:05 -0500


    This commit fixes mixedCluster and rolling upgrades by spoofing OpenSearch

    version 1.0.0 as Legacy version 7.10.2. With this commit an OpenSearch 1.x node
     can join a legacy (&lt;= 7.10.2) cluster and rolling upgrades work as expected.

    Mixed clusters will not work beyond the duration of the upgrade since shards

    cannot be replicated from upgraded nodes to nodes running older versions.
    Signed-off-by: Nicholas Walter Knize &lt;nknize@apache.org&gt;
    Co-authored-by: Shweta Thareja &lt;tharejas@amazon.com&gt;


* __[BUG] fix MainResponse to spoof version number for legacy clients (#708)__

    [Nick Knize](mailto:nknize@gmail.com) - Fri, 28 May 2021 10:07:06 -0500


    This commit changes MainResponse to spoof OpenSearch 1.x version numbers as

    Legacy version number 7.10.2 for legacy clients.
    Signed-off-by: Nicholas Walter Knize &lt;nknize@apache.org&gt;

* __[CVE] Upgrade dependencies for Azure related plugins to mitigate CVEs (#688) (#771) (#784)__

    [Abbas Hussain](mailto:abbas_10690@yahoo.com) - Thu, 27 May 2021 00:43:40 +0530


    * Update commons-io-2.4.jar to 2.7 for plugins/discovery-azure-classic module
    * Remove unused jackson dependency and respective LICENSE and NOTICE
    * Update guava dependency to mitigate CVE for repository-azure plugin
     Signed-off-by: Abbas Hussain &lt;abbas_10690@yahoo.com&gt;

* __distribution/packages: Fix RPM architecture name for 64-bit x86 (#620) (#770) (#783)__

    [Abbas Hussain](mailto:abbas_10690@yahoo.com) - Wed, 26 May 2021 23:55:35 +0530


    RPM uses the &#34;x86_64&#34; name for 64-bit x86, which is in-line with GCC
    and other
    compilers.
     Signed-off-by: Neal Gompa &lt;ngompa13@gmail.com&gt;

* __[Bug] Fix gradle build on Windows failing from a recent change (#765)__

    [Rabi Panda](mailto:adnapibar@gmail.com) - Tue, 25 May 2021 10:51:53 -0700


    A recent change as part of the commit c2e816ec introduced a bug where the build
    is failing on Windows. The change was made to include the NOTICE.txt file as
    read-only in the distributions. The code fails on Windows as it&#39;s not a
    POSIX-compliant. This commit adds a check on the current operating system.
     Signed-off-by: Rabi Panda &lt;adnapibar@gmail.com&gt;

* __Add timeout on cat/stats API (#759)__

    [Dhwanil Patel](mailto:dhwanip@amazon.com) - Mon, 24 May 2021 23:18:04 +0530


    Signed-off-by: Dhwanil Patel &lt;dhwanip@amazon.com&gt;

* __Remove URL content from Reindex error response (#630) (#748)__

    [Abbas Hussain](mailto:abbas_10690@yahoo.com) - Fri, 21 May 2021 17:20:34 -0700


    Signed-off-by: Sooraj Sinha &lt;soosinha@amazon.com&gt;
     Co-authored-by: Sooraj Sinha &lt;81695996+soosinha@users.noreply.github.com&gt;

* __Catch runtime exceptions to make class loader race conditions easier to debug. (#608) (#750)__

    [Abbas Hussain](mailto:abbas_10690@yahoo.com) - Fri, 21 May 2021 11:04:14 +0530


    Signed-off-by: dblock &lt;dblock@amazon.com&gt;
     Co-authored-by: Daniel Doubrovkine (dB.) &lt;dblock@dblock.org&gt;

* __distribution/packages: Fix filename format for deb archives (#621) (#753)__

    [Abbas Hussain](mailto:abbas_10690@yahoo.com) - Fri, 21 May 2021 11:03:36 +0530


    Debian packages are formatted with the following filename structure:
     name_[epoch:]version-release_arch.deb
     Make generated Debian packages follow this convention.
     Signed-off-by: Neal Gompa &lt;ngompa13@gmail.com&gt;
     Co-authored-by: Neal Gompa (ニール・ゴンパ) &lt;ngompa13@gmail.com&gt;

* __Update/maintainers.md (#723) (#752)__

    [Abbas Hussain](mailto:abbas_10690@yahoo.com) - Fri, 21 May 2021 04:45:28 +0530

    Adding Gopala, Vengad, Shweta, db and Itiyama to maintainers list
     Signed-off-by: CEHENKLE &lt;henkle@amazon.com&gt;

* __Updating README and CONTRIBUTING guide to get ready for beta1 release. (#672) (#751)__

    [Abbas Hussain](mailto:abbas_10690@yahoo.com) - Thu, 20 May 2021 15:57:07 -0700


    * Updating README and CONTRIBUTING guide to get ready for beta1 release.
     Signed-off-by: Sarat Vemulapalli &lt;vemulapallisarat@gmail.com&gt;

    * Addressing comments.
     Signed-off-by: Sarat Vemulapalli &lt;vemulapallisarat@gmail.com&gt;
     Co-authored-by: Sarat Vemulapalli &lt;vemulapallisarat@gmail.com&gt;

* __Update issue template with multiple labels (#668) (#749)__

    [Abbas Hussain](mailto:abbas_10690@yahoo.com) - Thu, 20 May 2021 15:55:56 -0700


    Signed-off-by: Vacha Shah &lt;vachshah@amazon.com&gt;
     Co-authored-by: Vacha Shah &lt;vachshah@amazon.com&gt;
     Co-authored-by: Vacha &lt;VachaShah@users.noreply.github.com&gt;
    Co-authored-by:
    Vacha Shah &lt;vachshah@amazon.com&gt;

* __Support Data Streams in OpenSearch (#690) (#713)__

    [Ketan Verma](mailto:ketanv3@users.noreply.github.com) - Thu, 20 May 2021 17:14:28 -0400


    This commit adds support for data streams by adding a DataStreamFieldMapper,
    and making timestamp
    field name configurable. Backwards compatibility is
    supported.
     Signed-off-by: Ketan Verma &lt;ketan9495@gmail.com&gt;

* __Make default number of shards configurable (#726)__

    [arunabh23](mailto:singharunabh18@gmail.com) - Thu, 20 May 2021 17:13:46 -0400


    The default number of primary shards for a new index, when the number of shards
    are not provided in the request, can be configured for the cluster. This is a
    backport commit of pull #625
     Signed-off-by: Arunabh Singh &lt;arunabs@amazon.com&gt;
     Co-authored-by: Arunabh Singh &lt;arunabs@amazon.com&gt;

* __[CVE-2020-7692] Upgrade google-oauth clients for goolge cloud plugins (#662) (#734)__

    [Rabi Panda](mailto:adnapibar@gmail.com) - Thu, 20 May 2021 17:13:15 -0400


    For discovery-gce and repository-gcs plugins update the google-oauth-client
    library to version 1.31.0. See CVE details at
    https://nvd.nist.gov/vuln/detail/CVE-2020-7692
     Signed-off-by: Rabi Panda &lt;adnapibar@gmail.com&gt;

* __Update dependencies for ingest-attachment plugin. (#666) (#735)__

    [Rabi Panda](mailto:adnapibar@gmail.com) - Thu, 20 May 2021 17:13:02 -0400


    This PR resolves the CVEs for dependencies in the ingest-attachment plugin.
     tika : &#39;1.24&#39; -&gt; &#39;1.24.1&#39;
    (https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2020-9489)
    pdfbox :
    &#39;2.0.19&#39; -&gt; &#39;2.0.23&#39;
    (https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-27807)

    commons-io:commons-io : &#39;2.6&#39; -&gt; &#39;2.7&#39;
    (https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-29425)
     Signed-off-by: Rabi Panda &lt;adnapibar@gmail.com&gt;

* __[CVE-2018-11765] Upgrade hadoop dependencies for hdfs plugin (#654) (#736)__

    [Rabi Panda](mailto:adnapibar@gmail.com) - Thu, 20 May 2021 17:12:33 -0400


    Hadoop 2.8.5 has been reported to have CVEs
    (https://bugzilla.redhat.com/show_bug.cgi?id=1883549). We need to upgrade this
    to 2.10.1. This also updates the hadoop-minicluster version to 2.10.1 as well.
    This upgrade also brings in two additional dependencies, woodstox-core and
    stax2-api that are added along with the sha1s, licenses and notices.
     Also upgrade guava to the latest as per the CVE
    https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2020-8908
     Signed-off-by: Rabi Panda &lt;adnapibar@gmail.com&gt;

* __[1.x] Add read_only block argument to opensearch-node unsafe-bootstrap command #599 (#725)__

    [Harmish](mailto:harmish.lakhani@gmail.com) - Thu, 20 May 2021 17:12:16 -0400


    * apply user defined cluster wide read only block after unsafe bootstrap
    command
     Signed-off-by: Harmish Lakhani &lt;harmish.lakhani@gmail.com&gt;

    * Fixed gradle precommit failures
     Signed-off-by: Harmish Lakhani &lt;harmish.lakhani@gmail.com&gt;

    * remove default as false for read only block to avoid overriding user&#39;s
    existing settings
     Signed-off-by: Harmish Lakhani &lt;harmish.lakhani@gmail.com&gt;

* __Handle inefficiencies while fetching the delayed unassigned shards during cluster health (#588) (#730)__

    [Abbas Hussain](mailto:abbas_10690@yahoo.com) - Thu, 20 May 2021 17:11:49 -0400


    Signed-off-by: Meet Shah &lt;meetshsh@gmail.com&gt;
     Co-authored-by: Meet Shah &lt;48720201+meetshah777@users.noreply.github.com&gt;

* __[TEST] Fix failing distro tests for linux packages (#569) (#733)__

    [Rabi Panda](mailto:adnapibar@gmail.com) - Thu, 20 May 2021 17:11:03 -0400


    Changes to fix the failing OpenSearch distribution tests for packages
    (linux-archive, linux-archive-aarch64, debian, rpm, docker) on supported linux
    distros.
     Signed-off-by: Rabi Panda &lt;adnapibar@gmail.com&gt;

* __Update instructions on debugging OpenSearch. (#689) (#738)__

    [Rabi Panda](mailto:adnapibar@gmail.com) - Thu, 20 May 2021 17:09:59 -0400


    Add clear instructions on how to run OpenSearch with debugging mode in
    IntelliJ.
    Fixed a few minor typos and grammars.
     Signed-off-by: Rabi Panda &lt;adnapibar@gmail.com&gt;

* __Create group settings with fallback. (#743) (#745)__

    [Daniel Doubrovkine (dB.)](mailto:dblock@dblock.org) - Thu, 20 May 2021 17:06:58 -0400


    * Create group settings with fallback.
     Signed-off-by: dblock &lt;dblock@amazon.com&gt;

    * Use protected fallbackSetting in Setting.
     Signed-off-by: dblock &lt;dblock@amazon.com&gt;

* __Make allocation decisions at node level first for pending task optimi… (#534) (#739)__

    [Ankit Jain](mailto:akjain@amazon.com) - Thu, 20 May 2021 14:02:18 -0700


    * Make allocation decisions at node level first for pending task optimization
     Signed-off-by: Ankit Jain &lt;akjain@amazon.com&gt;

    * Addressing review comments
     Signed-off-by: Ankit Jain &lt;akjain@amazon.com&gt;

    * Fixing benchmark and adding debug mode tests
     Signed-off-by: Ankit Jain &lt;akjain@amazon.com&gt;

    * Fixing typo in previous commit
     Signed-off-by: Ankit Jain &lt;akjain@amazon.com&gt;

    * Moving test file to correct package
     Signed-off-by: Ankit Jain &lt;akjain@amazon.com&gt;

    * Addressing review comments
     Signed-off-by: Ankit Jain &lt;akjain@amazon.com&gt;

* __[CVE] Upgrade dependencies to mitigate CVEs (#657) (#737)__

    [Rabi Panda](mailto:adnapibar@gmail.com) - Wed, 19 May 2021 21:20:53 -0700


    This PR upgrade the following dependencies to fix CVEs.

    - commons-codec:1.12 (-&gt;1.13) apache/commons-codec@48b6157
    - ant:1.10.8 (-&gt;1.10.9) https://ant.apache.org/security.html
    - jackson-databind:2.10.4 (-&gt;2.11.0) FasterXML/jackson-databind#2589
    - jackson-dataformat-cbor:2.10.4 (-&gt;2.11.0)
    https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2020-28491
    - apache-httpclient:4.5.10 (-&gt;4.5.13)
    https://bugzilla.redhat.com/show_bug.cgi?id=CVE-2020-13956
    - checkstyle:8.20 (-&gt;8.29)
    https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2019-10782
    - junit:4.12 (-&gt;4.13.1)
    https://github.com/junit-team/junit4/security/advisories/GHSA-269g-pwp5-87pp
    - netty:4.1.49.Final (-&gt;4.1.59)
    https://github.com/netty/netty/security/advisories/GHSA-5mcr-gq6c-3hq2
     Signed-off-by: Rabi Panda &lt;adnapibar@gmail.com&gt;

* __Added a link to the maintainer file in contribution guides (#589) (#731)__

    [Abbas Hussain](mailto:abbas_10690@yahoo.com) - Wed, 19 May 2021 13:41:32 -0700


    Co-authored-by: Dawn Foster &lt;fosterd@vmware.com&gt;

* __Fix snapshot deletion task getting stuck in the event of exceptions (#629) (#650)__

    [amitai stern](mailto:amitai.stern@logz.io) - Thu, 13 May 2021 09:02:57 -0500


    Changes the behavior of the recursive deletion function
    `executeOneStaleIndexDelete()` stop
    condition to be when the queue of
    `staleIndicesToDelete` is empty -- also in the error flow.
    Otherwise the
    GroupedActionListener never responds and in the event of a few exceptions the

    deletion task gets stuck.
    Alters the test case to fail to delete in bulk many
    snapshots at the first attempt, and then
    the next successful deletion also
    takes care of the previously failed attempt as the test
    originally intended.

    SNAPSHOT threadpool is at most 5. So in the event we get more than 5 exceptions
    there are no
    more threads to handle the deletion task and there is still one
    more snapshot to delete in the
    queue. Thus, in the test I made the number of
    extra snapshots be one more than the max in the
    SNAPSHOT threadpool.
     Signed-off-by: AmiStrn &lt;amitai.stern@logz.io&gt;

* __Standardize int, long, double and float Setting constructors. (#665)__

    [Daniel Doubrovkine (dB.)](mailto:dblock@dblock.org) - Thu, 6 May 2021 21:17:49 +0000




* __Converted all .asciidoc to .md. (#658)__

    [Daniel Doubrovkine (dB.)](mailto:dblock@dblock.org) - Thu, 6 May 2021 20:12:49 +0000


    Signed-off-by: dblock &lt;dblock@amazon.com&gt;

* __Fix #649: Properly escape @ in JavaDoc. (#651)__

    [Daniel Doubrovkine (dB.)](mailto:dblock@dblock.org) - Thu, 6 May 2021 20:12:22 +0000


    Signed-off-by: dblock &lt;dblock@amazon.com&gt;

* __Make -Dtests.output=always actually work. (#648)__

    [Daniel Doubrovkine (dB.)](mailto:dblock@dblock.org) - Thu, 6 May 2021 20:11:33 +0000


    Signed-off-by: dblock &lt;dblock@amazon.com&gt;

* __[WIP] Developer guide updates (#595)__

    [Daniel Doubrovkine (dB.)](mailto:dblock@dblock.org) - Wed, 28 Apr 2021 17:28:17 +0000


    * Add detail on how to install Java.
     Signed-off-by: dblock &lt;dblock@amazon.com&gt;

    * There&#39;s no password requirement for the instance.
     Signed-off-by: dblock &lt;dblock@amazon.com&gt;

    * Explain how to listen on an external IP.
     Signed-off-by: dblock &lt;dblock@amazon.com&gt;

* __Speedup snapshot stale indices delete (#613) (#616)__

    [Nick Knize](mailto:nknize@gmail.com) - Wed, 28 Apr 2021 11:02:21 -0500


    Instead of snapshot delete of stale indices being a single threaded operation
    this commit makes
    it a multithreaded operation and delete multiple stale
    indices in parallel using SNAPSHOT
    threadpool&#39;s workers.
     Signed-off-by: Piyush Daftary &lt;piyush.besu@gmail.com&gt;

* __Speedup lang-painless tests (#605) (#617)__

    [Nick Knize](mailto:nknize@gmail.com) - Wed, 28 Apr 2021 11:01:39 -0500


    Painless tests would previously create script engine for every single test
    method.
    Now the tests that need to tweak script engine settings create a class
     level fixture (BeforeClass/AfterClass) that is used across all the test

    methods in that suite.
     RegexLimitTests was split into two suites (limit=1 and limit=2) rather
    than
    dynamically applying different settings.
     C2 compiler is no longer needed for tests to be fast, instead tests run

    faster with C1 only as expected, like the rest of the unit tests.
     Signed-off-by: Robert Muir &lt;rmuir@apache.org&gt;

* __Replace elastic.co with opensearch.org (#611) (#623)__

    [Nick Knize](mailto:nknize@gmail.com) - Wed, 28 Apr 2021 10:59:36 -0500


    Signed-off-by: Abbas Hussain &lt;abbas_10690@yahoo.com&gt;
