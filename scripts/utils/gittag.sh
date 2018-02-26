#!/bin/bash

#$1=filename
#$2=version

if [ -z $1 ]
  then
    echo "
           Usage:\"gittag <filename> <version>\""
else
git tag -a v$2 -m "tagging '$1' '$2'"
git tag
echo "run \"git push origin cloudstore\""
fi