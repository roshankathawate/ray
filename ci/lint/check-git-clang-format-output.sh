#!/bin/bash

# Sometimes the default branch is not master, and I hope this script can also run in this case.
: "${DEFAULT_BRANCH:=master}"

# Compare against the $DEFAULT_BRANCH branch, because most development is done against it.
base_commit="$(git merge-base HEAD $DEFAULT_BRANCH)"
if [ "$base_commit" = "$(git rev-parse HEAD)" ]; then
  # Prefix of $DEFAULT_BRANCH branch, so compare against parent commit
  base_commit="$(git rev-parse HEAD^)"
  echo "Running clang-format against parent commit $base_commit"
else
  echo "Running clang-format against parent commit $base_commit from $DEFAULT_BRANCH branch"
fi

exclude_regex="(.*thirdparty/|.*redismodule.h|.*.java|.*.jsx?|.*.tsx?)"
output="$(ci/lint/git-clang-format --commit "$base_commit" --diff --exclude "$exclude_regex")"
if [ "$output" = "no modified files to format" ] || [ "$output" = "clang-format did not modify any files" ] ; then
  echo "clang-format passed."
  exit 0
else
  echo "clang-format failed:"
  echo "$output"
  exit 1
fi
