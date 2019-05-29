#!/bin/bash -xue

# configure Jenkins GitHub user
git config --global user.email "ci@camunda.com"
git config --global user.name "camunda-jenkins"

# setup maven central gpg keys
gpg -q --allow-secret-key-import --import --no-tty --batch --yes ${GPG_SEC_KEY}
gpg -q --import --no-tty --batch --yes ${GPG_PUB_KEY}
rm ${GPG_SEC_KEY} ${GPG_PUB_KEY}
