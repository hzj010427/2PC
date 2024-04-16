#!/bin/bash

# 解压 tar 包
tar -xvf test.tar

# 切换到 test 目录
cd test

# 设置 CLASSPATH 环境变量
export CLASSPATH="$PWD:$PWD/../lib:$PWD/../src"

# 这里可以添加其他需要运行的命令

