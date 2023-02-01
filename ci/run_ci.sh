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

# qpid-proton is pinned because latest version makes electron panic with:
# cannot marshal string: overflow: not enough space to encode
dnf downgrade -y https://cbs.centos.org/kojifiles/packages/qpid-proton/0.35.0/3.el8s/x86_64/qpid-proton-c-0.35.0-3.el8s.x86_64.rpm https://cbs.centos.org/kojifiles/packages/qpid-proton/0.35.0/3.el8s/x86_64/qpid-proton-c-devel-0.35.0-3.el8s.x86_64.rpm


# run unit tests
go test -v tests/*
