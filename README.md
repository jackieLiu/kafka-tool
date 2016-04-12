# kafka-tool
模拟kafka客户端
## 源码编译
项目使用gradle方式编译,由于在公司内部开发,在公网环境时需要修改仓库地址,修改 `build.gradle` 中repositories下的 `maven-->url`

1.  执行编译
``` gradle build -x test```
2. 执行打包(将编译后的内容打包成zip包)
``` gradle zipRelease -x test```
    
## 工具使用   
 
模拟发送json字符串。
