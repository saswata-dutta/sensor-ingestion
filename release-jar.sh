#!/usr/bin/env bash

set -xv

cd target/scala-*/

files=(predictive-maintenance*.jar)
jar_file="${files[0]}"

sha=$(git log --pretty=format:'%h' -n1)
ts=$(date +%s)
jar_name="${jar_file%.jar}"
dev_jar_name="${jar_name}_${sha}_${ts}.jar"

cp "$jar_file" "../../${dev_jar_name}"
