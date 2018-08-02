#!/bin/bash -e

THIS=$0

CUR_DIR=`pwd`
cd `dirname $THIS`
TARGET_FILE=`basename $THIS`
while [ -L "$TARGET_FILE" ]; do
    TARGET_FILE=`readlink $TARGET_FILE`
    cd `dirname $TARGET_FILE`
    TARGET_FILE=`basename $TARGET_FILE`
done
SC_DIR=`pwd -P`
cd "$CUR_DIR"

exec $SC_DIR/../_build/default/rel/raft/bin/raft -eval "raft_es:start($1)"

