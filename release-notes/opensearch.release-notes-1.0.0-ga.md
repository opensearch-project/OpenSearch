## Version 1.0.0-GA Release Notes

* __Apply fix for health API response to distinguish no master (#819)__

    [Mohit Godwani](mailto:81609427+mgodwan@users.noreply.github.com) - Fri, 25 Jun 2021 11:05:13 -0500

    efs/remotes/upstream/1.0, refs/remotes/origin/1.0, refs/heads/1.0
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
    Signed-off-by: dblock
    &lt;dblock@amazon.com&gt;
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


