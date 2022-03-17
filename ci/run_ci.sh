#!/bin/env bash
# CI script for CentOS8 jobs
set -ex

# enable required repo(s)
yum install -y epel-release

# Locale setting in CentOS8 is broken without this package
yum install -y glibc-langpack-en

# install Go
yum install -y golang
export GOBIN=$GOPATH/bin
export PATH=$PATH:$GOBIN

# install apputils dependencies
yum install -y epel-release
yum install -y qpid-proton-c-devel git

# run unit tests
go test -v tests/*
