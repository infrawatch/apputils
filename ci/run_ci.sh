#!/bin/env bash
# CI script for CentOS8 jobs
set -ex

# enable required repo(s)
yum install -y epel-release

# Locale setting in CentOS8 is broken without this package
yum install -y glibc-langpack-en

# install Go 1.24.13
GO_VERSION=1.24.13
curl -L "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" -o /tmp/go.tar.gz
tar -C /usr/local -xzf /tmp/go.tar.gz
export PATH=/usr/local/go/bin:$PATH
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
