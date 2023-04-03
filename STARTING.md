<p align="center"><img src="https://opensearch.org/assets/img/opensearch-logo-themed.svg" height="64px"></p>
<h1 align="center">OpenSearch start contributing guide</h1>

This guide applies to all repositories of the OpenSearch project and should help beginners to join contributing.

- [Getting started guide](#getting-started-guide)
  - [Choose a repository](#choose-a-repository)
  - [Install development environment](#install-development-environment)
  - [Contribute!](#contribute)
- [Contributing](#contributing)
  - [What actions contribute](#what-actions-contribute)
    - [Issues](#issues)
    - [Pull requests](#pull-requests)
  - [Github](#github)
  - [Git](#git)
- [First pull request](#first-pull-request)
  - [Changelog](#changelog)

## Getting started guide

This guide is intended for all those who want to start contributing.

It is distinguished by the fact that the information is summarised and will help you move forward from the start.

This guide describes superficial cases to understand how contributing works
You should definitely familiarize yourself with [contributing guide](CONTRIBUTING.md) after studying this markdown (.md).

### Choose a repository

Opensearch has many [repositories](https://github.com/opensearch-project).
You can choose any of them and start contributing.

Here are some options to help you decide which repository to choose:

- **Choose a repository that you find interesting to explore and develop**
- **Choose a repository associated with your technology stack**
- **Choose a repository that has enough issues to solve**
- **Choose a repository that does not have enough problems to solve if you like to suggest new features or find issues to solve**

### Install development environment

You have chosen a repository.

Don't forget to make a fork of the repository you've chosen, it's written about in _DEVELOPER_GUIDE.md_

The development environment (project) must now be installed to start making a contribution.
On the repository page you can find and read [developer guide](DEVELOPER_GUIDE.md)

### Contribute!

Have you installed and been able to run the development environment?

**Congratulations! You can start contributing!**

## Contributing

### What actions contribute

#### Issues

**Issues** is a functionality on the GitHub platform that allows users to track and manage tasks, bugs, feature requests and <br> other project-related issues. It provides a collaborative environment where developers can discuss and solve problems related to a particular repository.

_Note_: Since you are a beginner, we advise you to start with simple issues.

For this there is a special label "<font color="SlateBlue">**Good first issue**</font>"

Warning! If you have selected an issue and want to get started, check if the issue is free. If for it has already been assigned <br> and there is no pull request for a long time, ask that user using @ (e.g @nickname) if he/she is willing to concede to you.

If the issue has recently been created and there are no appointees, then feel free to write in a comment that you are ready to take on the issue.

Feel free to create an issue if you have not been able to work out the installation, or a bug within the project. Also feel free to ask on the issue page if you are solving a problem but can't figure out the structure of the project.

#### Pull requests

**Pull requests (PR)** is a feature on the GitHub platform that allows developers to suggest changes to project code, after which other developers can review, discuss and accept the changes. A PR allows you to review code, discuss it, fix bugs, and only then merge changes into the main project branch. This avoids conflicts and bugs in the main project code. PR is an important tool for project collaboration on GitHub.

_Note_: To create a pull request, you need to make a git push to the fork branch of the repository. For this, see [git](#git)

### Github

Before you can start creating pull requests you need to change some Github profile settings:

1. Change the **public email** to the one you will use to sign commits, just go to [profile settings](https://github.com/settings/profile)
2. Allow all actions for your forked repository:

   Go to your forked repository page

   Go to settings -> actions -> general and under `Actions permissions` check `Allow all cations and reusable workflows`

### Git

#### Install `git`

If you don't already have it installed (check with `git --version`) we recommend following the [the `git` installation guide for your OS](https://git-scm.com/downloads).

#### Configuration

We require that every contribution to OpenSearch is signed with a Developer Certificate of Origin. Additionally, please use your real name. We do not accept anonymous contributors nor those utilizing pseudonyms.

Each commit must include a DCO which looks like this

`Signed-off-by: Jane Smith <jane.smith@email.com>`

So we need to set up the data that will be used to sign our commits.

Fill in your personal details as well as an email that is public on github:

```
git config --global user.name "FIRST_NAME LAST_NAME"
git config --global user.email "MY_NAME@example.com"
```

_Note_: If you want to set up this data only for a specific repository then do it without the `--global` parameter:

```
git config user.name "FIRST_NAME LAST_NAME"
git config user.email "MY_NAME@example.com"
```

## First pull request

Done! Let's look at what you need to create your first Pull Request!

To fully work with the repository, you need to know how to use git
But to solve a simple issue, and create a pull request for it, you only need to know a few important git commands:

After you have done a fork and cloned the repository, you can modify the code and save it to the index.

But before all this, make sure you are on the main branch by calling the command in the repository directory:

```
git branch
```

The terminal will show a list of branches, with the current branch shown as '*' (e.g. *main)

If you only see one branch in the list main, you are doing the right thing

We recommend that you create a branch for each pull request of the same issue and only if you are on the main branch.
To do this, switch to the main branch:

```
git checkout main
```

_Note_: only create a new branch if you are on the main branch! And if you want to save commit history for another pull request, only then can you create a branch from any other branch

We changed the code, now we want to save the changes to a new branch that will be included in the pull request, so let's create a new branch and switch to it.

Creating a branch:

```
git branch <new-branch-for-pr>
```

Switching to the branch created:

```
git checkout new-branch-for-pr
```

_Note_: the previous two commands can be replaced by one:

```
git checkout -b new-branch-for-pr
```

The `-b` flag allows you to switch to a branch by creating it

You have switched to the created branch, now you can add the files that were changed to the index and do a `commit`:
Before doing so, let's check the status of our changes:

```
git status
```

_Note_: The `git status` command allows you to see what status our files are in

If you have created a file, you will see that it is `untracked`.
If you modified an existing file, you will see the status as `not staged`.

To add files to the list to be added to commit, run the command:

```
git add <src>
```

`<src>` is the path to the file. Executing `git status` will show all paths

_Note_: Text editors have a built-in git system. There you can control the status of changes

For example in VS code you can open the **Source Control** panel with the shortcut:

```
 Ctrl + Shift + G
```

If you have changed several files, you can add them with one command:

```
git add .
```

We have added all our changes to the index, now we can "Save" our changes by making a commit:

```
git commit -s -m "My first commit and commit message"
```

With this command, we initialize all our files that we've changed into a commit.

**Warning!**

The `-s` flag is required! It is required for the Developer Certificate of Origin (DCO).
It will sign your commit with the data you added to the git config.

The `-m` flag is also required! This is needed to succinctly describe changes to your files. Please keep your descriptions short and to the point.

Once you've committed, you need to submit your changes to a remote repository (forked repository):

```
git push -u origin new-branch-for-pr
```

The `git push` command transfers all local changes to the remote repository, in this case we have specified `origin`.

The `origin` is our remote repository (forked repository) that is copied and located on your github account.
There is also a source for `upstream` - but this is a read-only repository (fetch/pull).

You can see all the sources with the command:

```
git remote -v
```

We have dealt with the selected source. Let's move on to the `-u` flag.

The `-u` flag in git `push origin -u` (or `git push -u origin`) sets the branch on the remote repository as "upstream" for the local branch. This means that the next time you use git push without specifying the remote branch name and local branch name, Git will automatically send changes to the branch on the remote repository that is set to upstream.

For example, if you are working on a local new-branch-for-pr and want to push changes to a remote repository on a branch with the same name, you can use:

```
git push -u origin new-branch-for-pr
```

This will tell Git that the local new-branch-for-pr branch is upstream to the remote origin/new-branch-for-pr branch. Next time you push into this branch, you can simply use:

```
git push
```

Once you have done a git push, go to your forked repository page. You will see a notification that a new branch has been added and an opportunity to make a pull request. Click on the green "Compare & pull request" button.

Describe your pull request. Also use `#` in Issues Resolved to specify an issue that can be resolved by your changes. (e.g #3027)

To set to checked in the Check List, insert `x` in []. Check result in preview mode.

Done! You have done a great job! Check the completed pull request form and create the pull request.

## Changelog

And finally, one last thing - create an entry in CHANGELOG.md:

1. Make a template-based change to the appropriate section of the CHANGELOG.md file.

2. Use the above commands to add CHANGELOG.md to your branch and do a push.

3. Check the `Update CHANGELOG.md` box on your pull request page.

Congratulations! You have made your first pull request.

After 2 reviewers have agreed to the changes, your pull request will be merged with the main branch.

Thank you for your efforts! Your contribution goes down in history, it's very cool!
