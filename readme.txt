技术架构: 
     前端: html+vue
     后端:  spring boot+spring+jpa+redis+websocket
     数据库及数据仓库:  mysql, hbase,hadoop
功能: 
     1. 注册登录,个人信息管理。 
     2. 关注，取消关注. 
    3. 点赞
    4. 发布博客
    5. 拉取博客. 
技术点: 
   2. HBase rowkey键设计: 
                表：
 weibo:content
	Rowkey：用户ID_时间戳
	列族：info
	ColumnLabel：info:content
	ColumnValue：微博内容
	Version：一个版本
weibo:relation
	Rowkey：用户ID
	列族：attends，fans
	ColumnLabel：attends：关注用户ID，fans：粉丝用户ID
	ColumnValue：用户ID
	Version：一个版本

weibo:receive_content_email
	Rowkey：用户ID
	列族：info
	ColumnLabel：info:关注用户ID
	ColumnValue：关注用户微博内容的Rowkey
	Version：1000
                           列族：
   1. 关注业务: 

          


解决的问题:





disable 'weibo:content'
drop 'weibo:content'
disable 'weibo:relations'
drop 'weibo:relations'
disable 'weibo:receive_content_email'
drop 'weibo:receive_content_email'


scan 'weibo:content'
scan 'weibo:relations'
scan 'weibo:receive_content_email'







