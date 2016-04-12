# kafka-tool
模拟发送kafka数据
## 源码编译
项目使用gradle方式编译,由于在公司内部开发,在公网环境时需要修改仓库地址，修`·build.gradle`文件中的下述内容。

```
repositories {
 	maven{ 
    	//修改为所使用仓库地址
		url "http://10.1.228.199:18081/nexus/content/groups/public/"  
    }
}
``` 

1.  执行编译   
```gradle build -x test```
2. 执行打包(将编译后的内容打包成zip包)   
```gradle zipRelease -x test```
    
## 工具使用   
 
模拟发送json字符串。
