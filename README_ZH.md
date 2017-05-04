
我们团队开发了另外一个有意思的项目 pika, pika 是一个兼容redis 协议的大容量的存储, 用来解决redis 内存不够的问题, 欢迎大家试试

由于团队精力有限, 目前Atlas 依据满足了公司大部分的需求, 因此很多Issue 我们很难再继续满足. 但是我们欢迎社区里面喜欢Atlas, 愿意给Atlas 做贡献的小伙伴可以一起来改善Atlas, 给Atlas 提交代码. 欢迎联系 chenzongzhi@360.cn
### 一、简介


Atlas是由 Qihoo 360公司Web平台部基础架构团队开发维护的一个基于MySQL协议的数据中间层项目。它在MySQL官方推出的MySQL-Proxy 0.8.2版本的基础上，修改了大量bug，添加了很多功能特性。目前该项目在360公司内部得到了广泛应用，很多MySQL业务已经接入了Atlas平台，每天承载的读写请求数达几十亿条。同时，有超过50家公司在生产环境中部署了Atlas，超过800人已加入了我们的开发者交流群，并且这些数字还在不断增加。
    
主要功能：

1.读写分离

2.从库负载均衡

3.IP过滤

4.自动分表

5.DBA可平滑上下线DB

6.自动摘除宕机的DB

### 二、Atlas相对于官方MySQL-Proxy的优势

1.将主流程中所有Lua代码用C重写，Lua仅用于管理接口

2.重写网络模型、线程模型

3.实现了真正意义上的连接池

4.优化了锁机制，性能提高数十倍

### 三、Atlas详细说明

[1.Atlas的安装](http://github.com/Qihoo360/Atlas/wiki/Atlas的安装)

[2.Atlas的运行及常见问题](http://github.com/Qihoo360/Atlas/wiki/Atlas的运行及常见问题)

[3.Atlas的分表功能简介](http://github.com/Qihoo360/Atlas/wiki/Atlas的分表功能简介)

[4.Atlas部分配置参数及原理详解](http://github.com/Qihoo360/Atlas/wiki/Atlas部分配置参数及原理详解)

[5.Atlas的架构](https://github.com/Qihoo360/Atlas/wiki/Atlas的架构)

[6.Atlas的性能测试](https://github.com/Qihoo360/Atlas/wiki/Atlas的性能测试)

[7.Atlas功能特点FAQ](https://github.com/Qihoo360/Atlas/wiki/Atlas功能特点FAQ)

[8.Atlas Sharding](https://github.com/Qihoo360/Atlas/wiki/Atlas-Sharding)

### 四、Atlas的需求及Bug反馈方式

如果用户在实际的应用场景中对Atlas有新的功能需求，或者在使用Atlas的过程中发现了bug，欢迎用户发邮件至g-infra@360.cn，与我们取得联系，我们将及时回复。另外有热心网友建立了QQ群326544838，开发者也已经加入，方便讨论。

### 五、名字来源

Atlas：希腊神话中双肩撑天的巨人，普罗米修斯的兄弟，最高大强壮的神之一，因反抗宙斯失败而被罚顶天。我们期望这个系统能够脚踏后端DB，为前端应用撑起一片天。

### 更多

atlas、pika以及其他技术请关注：我们360私有云（HULK平台）平台微信公共号
<img src="http://i.imgur.com/pL4ni57.png" height = "400" width = "600" alt="2">

