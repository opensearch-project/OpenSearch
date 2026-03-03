---
name: Indexer-Dashboard compatibility testing with OpenSearch
about: Issue to perform internal testing of Indexer-Dashboard packages under a new version of OpenSearch
title: 'Indexer-Dashboard testing under OpenSearch (version)'
labels: request/operational, level/task, type/test
assignees: ''

---


## Description

We need to ensure our components work under the new version of OpenSearch. The goal of this issue is to test our packages, their lifecycle and the main correct communication of Indexer and Dashboard.

For that, we need to:

- [x] (Prerequisite) \<indexer-opensearch-compatibility-issue> 
- [x] (Prerequisite) \<dashboard-opensearch-compatibility-issue>
- [ ] Verify the packages installs
- [ ] Verify the package upgrades: \<from-version> ⇾ \<to-version>
- [ ] Indexer-Dashboard communication works


Tests must be performed following the official documentation under RHEL 9 and Ubuntu 22.04 operating systems, or newer versions if available and supported.

## Issues
-  _List here the detected issues_
