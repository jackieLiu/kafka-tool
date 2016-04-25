@echo off & setlocal enabledelayedexpansion

set LIB_JARS=..\lib\*

java -Xms64m -Xmx256m -XX:MaxPermSize=64M -classpath ../;%LIB_JARS% com.lt.tools.kafka.KafkaProduce

:end
pause