#!/bin/bash

git log --graph --abbrev-commit -n3

./dist/target/zeebe-broker/bin/zbctl version
