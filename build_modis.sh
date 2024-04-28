#!/bin/bash

# 定义变量以获取当前的 Git commit hash, 分支名, 最新的commit信息和构建时间戳
COMMIT_HASH=$(git rev-parse HEAD)
BRANCH_NAME=$(git rev-parse --abbrev-ref HEAD)
BUILD_TIME=$(date "+%Y-%m-%d %H:%M:%S")
LAST_COMMIT_LOG=$(git log -1 --pretty=%B)
GO_VERSION=$(go version | awk '{print $3}') # 例如 "go1.15.2"

# 构建 Go 程序，注入 Git commit hash, 分支名, 构建时间, commit log 和 Go 版本
cd cmd/modis
go mod tidy
go mod verify
go build -ldflags "-X 'main.CommitHash=$COMMIT_HASH' \
-X 'main.BranchName=$BRANCH_NAME' \
-X 'main.BuildTS=$BUILD_TIME' \
-X 'main.CommitLog=$LAST_COMMIT_LOG' \
-X 'main.GolangVersion=$GO_VERSION'"

# 检查构建是否成功
if [ $? -eq 0 ]; then
    cd ../..
    mv -f cmd/modis/modis .
    echo "Build successful."
    echo "Git Commit Hash: $COMMIT_HASH"
    echo "Git Branch Name: $BRANCH_NAME"
    echo "Build Time: $BUILD_TIME"
    echo "Last Commit Log: $LAST_COMMIT_LOG"
    echo "Go Version: $GO_VERSION"
else
    echo "Build failed"
    exit 1
fi
