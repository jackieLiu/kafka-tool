# kafka-tool
模拟发送kafka数据
## 源码编译
项目使用gradle方式编译.

1.  执行编译   
```gradle build -x test```
2. 执行打包(将编译后的内容打包成zip包)   
```gradle zipRelease -x test```
    
## 工具使用   
 
现支持发送string字符和json内容.在发送之前先修改`kafka.properties`文件中的`val.type`.
