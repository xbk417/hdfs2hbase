	本项目中包含有两个程序，分别对应HBase表数据导出到Hdfs、查询HBase表中最大时间戳即最新更新的数据等两个功能。
	项目中包目录对应情况：
		com.bonc.hbase.hbase2hdfs：HBase表数据导出到Hdfs
		com.bonc.hbase.maxtimestamp：查询HBase表中最大时间戳即最新更新的数据

## 1. HBase表数据导出到Hdfs ##
#### 功能 ####
	HBase表数据导出到Hdfs
#### 程序使用说明 ####
	程序主类为：com.bonc.hbase.hbase2hdfs.HBase2HdfsExport
	程序参数列表为：
		<tablename>：表名
		<outputdir>：输出路径
		<configpath>：配置文件路径
		-specid：指定的specid
		-starttime：限定时间范围的开始时间
		-endtime：限定时间范围的结束时间

## 2. 查询HBase表中最大时间戳即最新更新的数据 ##
#### 功能 ####
	查询HBase表中最大时间戳即最新更新的数据。
#### 程序使用说明 ####
	程序主类为：com.bonc.hbase.maxtimestamp.MaxTimestamp
	程序参数列表为：<表名> <输出路径> <配置文件路径> <job名>