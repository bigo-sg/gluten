---
layout: page
title: Gluten For Flink with Velox Backend
nav_order: 1
---

# Supported Version

| Type  | Version                      |
|-------|------------------------------|
| Flink | 1.19.2                       |
| OS    | Ubuntu20.04/22.04, Centos7/8 |
| jdk   | openjdk11/jdk17              |
| scala | 2.12                         |

# Prerequisite

Currently, with static build Gluten+Flink+Velox backend supports all the Linux OSes, but is only tested on **Ubuntu20.04**. With dynamic build, Gluten+Velox backend support **Ubuntu20.04/Ubuntu22.04/Centos7/Centos8** and their variants.

Currently, the officially supported Flink version is 1.19.2.

We need to set up the `JAVA_HOME` env. Currently, Gluten supports **java 11** and **java 17**.

**For x86_64**

```bash
## make sure jdk11 is used
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

**For aarch64**

```bash
## make sure jdk11 is used
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
export PATH=$JAVA_HOME/bin:$PATH
```

**Get Velox4j**

Gluten for Flink depends on [Velox4j](https://github.com/velox4j/velox4j) to call velox. This is an experimental feature.
You need to get the Velox4j code, and compile it first.

```bash
## fetch velox4j code
git clone https://github.com/bigo-sg/velox4j.git
cd velox4j
git checkout gluten
mvn clean install
```
**Get gluten**

```bash
## config maven, like proxy in ~/.m2/settings.xml

## fetch gluten code
git clone https://github.com/apache/incubator-gluten.git
```

# Build Gluten Flink with Velox Backend

```
cd /path/to/gluten/gluten-flink
mvn clean package 
```

## Dependency library deployment

You need to get the Velox4j packages and used them with gluten.
Velox4j jar available now is velox4j-0.1.0-SNAPSHOT.jar. 

## Submit the Flink SQL job

Submit test script from `flink run`. You can use the `StreamSQLExample` as an example. 

### Flink local cluster

After deploying flink binaries, please add gluten-flink jar to flink library path,
including gluten-flink-runtime-1.4.0.jar, gluten-flink-loader-1.4.0.jar and Velox4j jars above.
And make them loaded before flink libraries.
Then you can go to flink binary path and use the below scripts to
submit the example job.

```bash
bin/start-cluster.sh
bin/flink run -d -m 0.0.0.0:8080 \
    -c org.apache.flink.table.examples.java.basics.StreamSQLExample \
    lib/flink-examples-table_2.12-1.20.1.jar
```

Then you can get the result in `log/flink-*-taskexecutor-*.out`.
And you can see an operator named `gluten-cal` from the web frontend of your flink job. 

#### All operators executed by native
Another example supports all operators executed by native. 
You can use the data-generator.sql in dev directory.

```bash
wget  https://archive.apache.org/dist/flink/flink-1.19.2/flink-1.19.2-bin-scala_2.12.tgz  
tar zxvf flink-1.19.1-bin-scala_2.12.tgz  
cd flink-1.19.2
ps -ef | grep flink  
bash -x deploy.sh  
bin/start-cluster.sh 
./bin/sql-client.sh -f data-generator.sql 
```

deploy.sh is a script to deploy the jars to the lib directory of Flink.
``` bash
#!/bin/bash

gluten_src_dir="/data1/liyang/cppproject/gluten/gluten-flink"
other_src_dir="/root/.m2/repository"
dst_dir="/data1/liyang/cppproject/spark/flink/flink-1.19.2/lib"

# 定义JAR包列表的数组
GLUTEN_JAR=(
    "$gluten_src_dir/loader/target/gluten-flink-loader-1.4.0-SNAPSHOT.jar"
    "$other_src_dir/io/github/zhztheplayer/velox4j/0.1.0-SNAPSHOT/velox4j-0.1.0-SNAPSHOT.jar"
    "$gluten_src_dir/runtime/target/gluten-flink-runtime-1.4.0-SNAPSHOT.jar"
)
OTHER_JAR=(
    "$other_src_dir/com/google/guava/guava/33.4.0-jre/guava-33.4.0-jre.jar"
    "$other_src_dir/com/fasterxml/jackson/core/jackson-core/2.18.0/jackson-core-2.18.0.jar"
    "$other_src_dir/com/fasterxml/jackson/core/jackson-databind/2.18.0/jackson-databind-2.18.0.jar"
    "$other_src_dir/com/fasterxml/jackson/datatype/jackson-datatype-jdk8/2.18.0/jackson-datatype-jdk8-2.18.0.jar"
    "$other_src_dir/com/fasterxml/jackson/core/jackson-annotations/2.18.0/jackson-annotations-2.18.0.jar"
    "$other_src_dir/org/apache/arrow/arrow-memory-core/18.1.0/arrow-memory-core-18.1.0.jar"
    "$other_src_dir/org/apache/arrow/arrow-memory-unsafe/18.1.0/arrow-memory-unsafe-18.1.0.jar"
    "$other_src_dir/org/apache/arrow/arrow-vector/18.2.0/arrow-vector-18.2.0.jar"
    "$other_src_dir/com/google/flatbuffers/flatbuffers-java/24.3.25/flatbuffers-java-24.3.25.jar"
    "$other_src_dir/org/apache/arrow/arrow-format/18.1.0/arrow-format-18.1.0.jar"
    "$other_src_dir/org/apache/arrow/arrow-c-data/18.1.0/arrow-c-data-18.1.0.jar"
)

# 合并两个数组
ALL_JARS=("${GLUTEN_JAR[@]}" "${OTHER_JAR[@]}")

# 遍历所有JAR文件路径
cd $dst_dir || exit

for jar in "${ALL_JARS[@]}"; do
    if [ ! -f "$jar" ]; then
        echo "File $jar does not exist."
        exit 1
    fi

    # 获取文件名
    filename=$(basename "$jar")

    # 创建符号链接
    ln -sf "$jar" "$filename"
    echo "Created symlink for $filename"
done
```

### Flink Yarn per job mode

TODO

## Performance
Using the data-generator example, it shows that for native execution, it can generate 10,0000
records in about 60ms, while Flink generator 10,000 records in about 600ms. It runs 10 times faster.
More perf cases to be added.

## Notes:
Now both Gluten for Flink and Velox4j have not a bundled jar including all jars depends on.
So you may have to add these jars by yourself, which may including guava-33.4.0-jre.jar, jackson-core-2.18.0.jar,
jackson-databind-2.18.0.jar, jackson-datatype-jdk8-2.18.0.jar, jackson-annotations-2.18.0.jar, arrow-memory-core-18.1.0.jar,
arrow-memory-unsafe-18.1.0.jar, arrow-vector-18.1.0.jar, flatbuffers-java-24.3.25.jar, arrow-format-18.1.0.jar, arrow-c-data-18.1.0.jar.
We will supply bundled jars soon.