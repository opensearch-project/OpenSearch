---
title: '[AUTOCUT] Gradle Check Failure on push to {{ env.branch_name }}'
labels: '>test-failure, bug'
---

Gradle check has failed on push of your commit {{ env.pr_from_sha }}.
Please examine the workflow log {{ env.workflow_url }}.
Is the failure [a flaky test](https://github.com/opensearch-project/OpenSearch/blob/main/DEVELOPER_GUIDE.md#flaky-tests) unrelated to your change?
