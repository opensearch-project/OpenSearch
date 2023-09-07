#!/usr/bin/python3
# SPDX-License-Identifier: Apache-2.0

import itertools
import json
import re
import sys
from github import Github

# We don't want to copy all labels on linked issues; only those in this subset.
COPYABLE_LABELS = {
  "enhancement",
  "documentation"
}

# Returns a pull request extracted from Github's event JSON.
def get_pr(event):
    # --- Extract PR from event JSON ---
    # `check_run`/`check_suite` does not include any direct reference to the PR
    # that triggered it in its event JSON. We have to extrapolate it using the head
    # SHA that *is* there.

    # Get head SHA from event JSON
    pr_head_sha = event["check_suite"]["head_sha"]

    # Find the repo PR that matches the head SHA we found
    return {pr.head.sha: pr for pr in repo.get_pulls()}[pr_head_sha]

# Get all issue numbers related to a PR.
def get_related_issues(pr):
    # Regex to pick out closing keywords.
    regex = re.compile("(close[sd]?|fix|fixe[sd]?|resolve[sd]?)\s*:?\s+#(\d+)", re.I)

    # Extract all associated issues from closing keyword in PR
    for verb, num in regex.findall(pr.body):
        yield int(num)

    # Extract all associated issues from PR commit messages
    for c in pr.get_commits():
        for verb, num in regex.findall(c.commit.message):
            yield int(num)

# Get inputs from shell
(token, repository, path) = sys.argv[1:4]

# Initialize repo
repo = Github(token).get_repo(repository)

# Open Github event JSON
with open(path) as f:
    event = json.load(f)

# Get the PR we're working on.
pr = get_pr(event)
pr_labels = {label.name for label in pr.labels}

# Get every label on every linked issue.
issues = get_related_issues(pr)
issues_labels = [repo.get_issue(n).labels for n in issues]
issues_labels = {l.name for l in itertools.chain(*issues_labels)}

# Find the set of all labels we want to copy that aren't already set on the PR.
unset_labels = COPYABLE_LABELS & issues_labels - pr_labels

# If there are any labels we need to add, add them.
if len(unset_labels) > 0:
    pr.set_labels(*list(unset_labels))
