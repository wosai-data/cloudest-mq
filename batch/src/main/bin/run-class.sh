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

if [ ! -z "$HADOOP_CONF_DIR" ]; then
  LIBS=$LIBS:$HADOOP_CONF_DIR
fi

echo classpath = $LIBS

logback_config=$basedir/conf/batch-logback.xml

echo logback_config = ${logback_config}

exec java -Dlogback.configurationFile=${logback_config} -cp $LIBS $@
