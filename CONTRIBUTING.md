# Contribution Guidelines

We'd love to accept your code patches!

## Code Styles

### Command Line Parsing

If you need to define, parse, and read command line flags, please use [Abseil's Flag library for Command Line Parsing](https://abseil.io/docs/cpp/guides/flags), which is easy to use and out of box.

### Pull Requests & Tests

They are useful, please use them. Travis-CI is built-in for pull requests, so they will tell you if your commit will break the code. Also, common sense, add tests whenever you commit large chunk of code.

### Clean Git History

Before you merge you local branch to master branch, please use `git rebase` when merging changes from the `master` branch and use `git rebase -i HEAD~<n>` to clean up or squash your local commit histories, so you don't have any funky duplicate commit messages, or unwanted commits.

### Commit Messages

Please, if possible, follow the commit message [style guide](https://dev.to/pavlosisaris/git-commits-an-effective-style-guide-2kkn):

```

[commit type] commit messages

body (changes, links, attributions, etc)

issue tracker id (for reference)

```

where `commit type` is one of `feat, fix, docs, style, refactor, chore, test` etc.


### C++ Style Guide

Please, if possible, follow [Google C++ style guide](https://google.github.io/styleguide/cppguide.html). If you use an IDE or any common text editors, they have extensions that help you auto format and lint your code for style errors.
