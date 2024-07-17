#!/bin/bash

source env.sh

# Build the Go program, inject Git commit hash, branch name, build time, commit log, and Go version
cd cmd/modis
go mod tidy
go mod verify
go build -ldflags  \
"-X 'main.GolangVersion=$GO_VERSION'\
-X 'github.com/oceanbase/modis/command.GitSha1=$GIT_SHA1'\
-X 'github.com/oceanbase/modis/command.GitDirty=$GIT_DIRTY'\
-X 'github.com/oceanbase/modis/command.BuildID=$BUILD_ID'\
-X 'github.com/oceanbase/modis/command.ModisVer=$VERSION'\
-X 'github.com/oceanbase/modis/command.CommitID=$COMMIT_HASH'\
"
# \ -gcflags "all=-N -l" # for debug

# Check whether the build was successful
if [ $? -eq 0 ]; then
    cd ../..
    mv -f cmd/modis/modis .
    echo "Build successful."
    echo "Git Commit Hash: $COMMIT_HASH"
    echo "Git Branch Name: $BRANCH_NAME"
    echo "Build Time: $BUILD_TIME"
    echo "Last Commit Log: $LAST_COMMIT_LOG"
    echo "Go Version: $GO_VERSION"
    echo "Git SHA1: $GIT_SHA1"
    echo "Git Dirty: $GIT_DIRTY"
    echo "Build ID: $BUILD_ID"
else
    echo "Build failed"
    exit 1
fi
