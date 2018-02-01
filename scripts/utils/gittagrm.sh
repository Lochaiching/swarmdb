#!/bin/bash

#$1=version

if [ -z $1 ]
  then
    echo "
           Usage:\"gittagrm <version>\""
else
echo "git tag -d '$1'
"
git tag -d $1
git tag
echo "running \"git push origin :refs/tags/'$1'\"
"
git push origin :refs/tags/$1
fi
