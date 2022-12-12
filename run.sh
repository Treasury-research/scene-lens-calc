#!/bin/bash
###
# @Author: zhouyong
# @File: run.sh
# @Time: 2022/12/12 17:59
# @Description: 工程描述
###
s3_path="s3://knn3-flink/jar"
project_path="/home/centos/jobs/zhouyong/package"
now=$(date +"%Y-%m-%d %H:%M:%S")
# yesterday=$(date -d "-1 day ${now}" "+%Y-%m-%d")
cd "$(dirname "$0")" || exit
filepath=$(pwd)
echo "当前时间: ${now} *** 进入 ${filepath} 目录"
# here put the import lib
git pull origin main
mvn clean package -Dmaven.test.skip=true
aws s3 cp "${project_path}"/scene-lens-calc/target/scene-lens-calc-jar-with-dependencies.jar "${s3_path}"/scene-lens-calc-jar-with-dependencies.jar
echo "上传到scene-lens-calc-jar-with-dependencies.jar到${s3_path}"
aws s3 cp "${project_path}"/scene-lens-calc/src/main/resources/SceneLensCalcJob.sh "${s3_path}"/SceneLensCalcJob.sh
echo "上传到SceneLensCalcJob.sh到${s3_path}"