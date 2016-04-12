@echo off & setlocal enabledelayedexpansion

set LIB_JARS=..\lib\*

java -Xms64m -Xmx512m -XX:MaxPermSize=64M -classpath ..\;%LIB_JARS% com.ai.opt.tools.kafka.KafkaProduce

:end
pause