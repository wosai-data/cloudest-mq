#!/bin/sh

workdir=`pwd`
basedir=`dirname $0`
cd $basedir/..
basedir=`pwd`
cd $workdir

echo basedir = $basedir
echo workdir = $workdir

LIBS=.
for f in $basedir/lib/*.jar
do
  LIBS=$LIBS:$f
done

echo classpath = $LIBS

java -cp $LIBS $@
