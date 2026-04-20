DROP TABLE IF EXISTS `slides`;
CREATE TABLE `slides` (
  `slides_id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '轮播图ID：',
  `title` varchar(64) CHARACTER SET utf8 DEFAULT NULL COMMENT '标题：',
  `content` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '内容：',
  `url` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '链接：',
  `img` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '轮播图：',
  `hits` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '点击量：',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间：',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间：',
  PRIMARY KEY (`slides_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='轮播图';

DROP TABLE IF EXISTS `upload`;
CREATE TABLE `upload` (
  `upload_id` int(11) NOT NULL AUTO_INCREMENT COMMENT '上传ID',
  `name` varchar(64) CHARACTER SET utf8 DEFAULT NULL COMMENT '文件名',
  `path` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '访问路径',
  `file` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '文件路径',
  `display` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '显示顺序',
  `father_id` int(11) DEFAULT '0' COMMENT '父级ID',
  `dir` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '文件夹',
  `type` varchar(32) CHARACTER SET utf8 DEFAULT NULL COMMENT '文件类型',
  PRIMARY KEY (`upload_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='文件上传';
insert into `upload` values ('1','movie.mp4','/upload/movie.mp4','',null,'0',null,'video');
DROP TABLE IF EXISTS `logistics_delivery`;
CREATE TABLE `logistics_delivery` (
  `logistics_delivery_id` int(11) NOT NULL AUTO_INCREMENT COMMENT '物流配送ID',
  `order_number` varchar(64) CHARACTER SET utf8 DEFAULT NULL COMMENT '订单号',
  `product_name` varchar(64) CHARACTER SET utf8 DEFAULT NULL COMMENT '商品名称',
  `purchase_quantity` varchar(64) CHARACTER SET utf8 DEFAULT NULL COMMENT '购买数量',
  `total_transaction_amount` double(10,2) DEFAULT '0.00' COMMENT '交易总额',
  `the_date_of_issuance` date DEFAULT NULL COMMENT '发货日期',
  `delivery_number` varchar(30) CHARACTER SET utf8 DEFAULT NULL COMMENT '配送订单',
  `ordinary_users` int(11) DEFAULT '0' COMMENT '普通用户',
  `shipping_address` varchar(64) CHARACTER SET utf8 DEFAULT NULL COMMENT '收货地址',
  `delivery_status` varchar(64) CHARACTER SET utf8 DEFAULT NULL COMMENT '配送状态',
  `signing_status` varchar(64) CHARACTER SET utf8 DEFAULT NULL COMMENT '签收状态',
  `recommend` int(11) NOT NULL DEFAULT '0' COMMENT '智能推荐',
  `contact_name` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '联系人名字',
  `merchant_id` int(11) DEFAULT NULL COMMENT '商家id',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`logistics_delivery_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='物流配送';
DROP TABLE IF EXISTS `goods`;
CREATE TABLE `goods` (
  `goods_id` mediumint(8) unsigned NOT NULL AUTO_INCREMENT COMMENT '产品id：[0,8388607]',
  `title` varchar(125) DEFAULT NULL COMMENT '标题：[0,125]用于产品和html的<title>标签中',
  `img` text COMMENT '封面图：用于显示于产品列表页',
  `description` varchar(255) DEFAULT NULL COMMENT '描述：[0,255]用于产品规格描述',
  `price_ago` double(8,2) unsigned NOT NULL DEFAULT '0.00' COMMENT '原价：[1]',
  `price` double(8,2) unsigned NOT NULL DEFAULT '0.00' COMMENT '卖价：[1]',
  `sales` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '销量：[0,1000000000]',
  `inventory` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '商品库存',
  `type` varchar(64) NOT NULL DEFAULT '' COMMENT '商品分类：',
  `hits` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '点击量：[0,1000000000]访问这篇产品的人次',
  `content` longtext COMMENT '正文：产品的主体内容',
  `img_1` text COMMENT '主图1：',
  `img_2` text COMMENT '主图2：',
  `img_3` text COMMENT '主图3：',
  `img_4` text COMMENT '主图4：',
  `img_5` text COMMENT '主图5：',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间：',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间：',
  `customize_field` text COMMENT '自定义字段',
  `source_table` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '来源表：',
  `source_field` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '来源字段：',
  `source_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '来源ID：',
  `user_id` int(11) unsigned DEFAULT '0' COMMENT '添加人',
  `integral` int(11) DEFAULT '0' COMMENT '积分',
  PRIMARY KEY (`goods_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='商品信息';
insert into `goods` values ('1','测试商品1','/upload/goods.jpg','测试商品1','110.0','99.0','0','80','分类一','0','<p>测试商品1</p>','/upload/img_1.jpg','/upload/img_2.jpg','/upload/img_3.jpg','/upload/img_4.jpg','/upload/img_5.jpg','2024-12-31 18:21:49.0','2024-12-31 15:52:28.0','[{"field_name":"品牌","field_value":""},{"field_name":"规格","field_value":""}]','mall_center','mall_center_id','5','1','0');
insert into `goods` values ('2','测试商品2','/upload/goods.jpg','测试商品2','220.0','110.0','0','10','分类二','0','<p>测试商品2</p>','/upload/img_1.jpg','/upload/img_2.jpg','/upload/img_3.jpg','/upload/img_4.jpg','/upload/img_5.jpg','2024-12-31 18:21:49.0','2024-12-31 15:52:28.0','[{"field_name":"品牌","field_value":""},{"field_name":"规格","field_value":""}]','mall_center','mall_center_id','6','1','0');
insert into `goods` values ('3','测试商品3','/upload/goods.jpg','测试商品3','20.0','10.0','0','10','分类二','0','<p>测试商品3</p>','/upload/img_1.jpg','/upload/img_2.jpg','/upload/img_3.jpg','/upload/img_4.jpg','/upload/img_5.jpg','2024-12-31 18:21:49.0','2024-12-31 15:52:28.0','[{"field_name":"品牌","field_value":""},{"field_name":"规格","field_value":""}]','mall_center','mall_center_id','7','1','0');
insert into `goods` values ('4','测试商品4','/upload/goods.jpg','测试商品4','199.0','99.0','0','20','分类二','0','<p>测试商品4</p>','/upload/img_1.jpg','/upload/img_2.jpg','/upload/img_3.jpg','/upload/img_4.jpg','/upload/img_5.jpg','2024-12-31 18:21:49.0','2024-12-31 15:52:28.0','[{"field_name":"品牌","field_value":""},{"field_name":"规格","field_value":""}]','mall_center','mall_center_id','8','1','0');
insert into `goods` values ('5','测试商品5','/upload/goods.jpg','测试商品5','110.0','99.0','0','30','分类二','0','<p>测试商品5</p>','/upload/img_1.jpg','/upload/img_2.jpg','/upload/img_3.jpg','/upload/img_4.jpg','/upload/img_5.jpg','2024-12-31 18:21:49.0','2024-12-31 15:52:28.0','[{"field_name":"品牌","field_value":""},{"field_name":"规格","field_value":""}]','mall_center','mall_center_id','9','1','0');
insert into `goods` values ('6','测试商品6','/upload/goods.jpg','测试商品6','999.0','888.0','0','40','分类三','0','<p>测试商品6</p>','/upload/img_1.jpg','/upload/img_2.jpg','/upload/img_3.jpg','/upload/img_4.jpg','/upload/img_5.jpg','2024-12-31 18:21:49.0','2024-12-31 15:52:28.0','[{"field_name":"品牌","field_value":""},{"field_name":"规格","field_value":""}]','mall_center','mall_center_id','10','1','0');
insert into `goods` values ('7','测试商品7','/upload/goods.jpg','测试商品7','20.0','220.0','0','110','分类一','0','<p>测试商品7</p>','/upload/img_1.jpg','/upload/img_2.jpg','/upload/img_3.jpg','/upload/img_4.jpg','/upload/img_5.jpg','2024-12-31 18:21:49.0','2024-12-31 16:21:46.0','[{"field_name":"品牌","field_value":""},{"field_name":"规格","field_value":""}]','mall_center','mall_center_id','11','1','0');
insert into `goods` values ('8','测试商品8','/upload/goods.jpg','测试商品8','20.0','30.0','0','30','分类二','0','<p>测试商品8</p>','/upload/img_1.jpg','/upload/img_2.jpg','/upload/img_3.jpg','/upload/img_4.jpg','/upload/img_5.jpg','2024-12-31 18:21:49.0','2024-12-31 15:52:28.0','[{"field_name":"品牌","field_value":""},{"field_name":"规格","field_value":""}]','mall_center','mall_center_id','12','1','0');
insert into `goods` values ('9','测试商品9','/upload/goods.jpg','测试商品9','40.0','35.0','0','20','分类三','0','<p>测试商品9</p>','/upload/img_1.jpg','/upload/img_2.jpg','/upload/img_3.jpg','/upload/img_4.jpg','/upload/img_5.jpg','2024-12-31 18:21:49.0','2024-12-31 15:52:28.0','[{"field_name":"品牌","field_value":""},{"field_name":"规格","field_value":""}]','mall_center','mall_center_id','13','1','0');
insert into `goods` values ('10','测试商品10','/upload/goods.jpg','测试商品10','50.0','45.0','0','80','分类一','0','<p>测试商品10</p>','/upload/img_1.jpg','/upload/img_2.jpg','/upload/img_3.jpg','/upload/img_4.jpg','/upload/img_5.jpg','2024-12-31 18:21:49.0','2024-12-31 15:52:28.0','[{"field_name":"品牌","field_value":""},{"field_name":"规格","field_value":""}]','mall_center','mall_center_id','14','1','0');
insert into `goods` values ('11','测试商品11','/upload/goods.jpg','测试商品11','60.0','50.0','0','10','分类二','0','<p>测试商品11</p>','/upload/img_1.jpg','/upload/img_2.jpg','/upload/img_3.jpg','/upload/img_4.jpg','/upload/img_5.jpg','2024-12-31 18:21:49.0','2024-12-31 15:52:28.0','[{"field_name":"品牌","field_value":""},{"field_name":"规格","field_value":""}]','mall_center','mall_center_id','15','1','0');
insert into `goods` values ('12','测试商品12','/upload/goods.jpg','测试商品12','70.0','60.0','0','30','分类三','0','<p>测试商品12</p>','/upload/img_1.jpg','/upload/img_2.jpg','/upload/img_3.jpg','/upload/img_4.jpg','/upload/img_5.jpg','2024-12-31 18:21:49.0','2024-12-31 15:52:28.0','[{"field_name":"品牌","field_value":""},{"field_name":"规格","field_value":""}]','mall_center','mall_center_id','16','1','0');
DROP TABLE IF EXISTS `cart`;
CREATE TABLE `cart` (
  `cart_id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '购物车ID：',
  `title` varchar(64) CHARACTER SET utf8 DEFAULT NULL COMMENT '标题：',
  `img` varchar(255) CHARACTER SET utf8 NOT NULL DEFAULT '0' COMMENT '图片：',
  `user_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '用户ID：',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间：',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间：',
  `state` int(1) unsigned NOT NULL DEFAULT '0' COMMENT '状态：使用中，已失效',
  `price` double(8,2) NOT NULL DEFAULT '0.00' COMMENT '单价：',
  `price_ago` double(8,2) NOT NULL DEFAULT '0.00' COMMENT '原价：',
  `price_count` double(10,2) NOT NULL DEFAULT '0.00' COMMENT '总价：',
  `num` int(8) unsigned NOT NULL DEFAULT '1' COMMENT '数量：',
  `goods_id` mediumint(8) unsigned NOT NULL COMMENT '商品id：[0,8388607]',
  `type` varchar(64) NOT NULL DEFAULT '未分类' COMMENT '商品分类：',
  `description` varchar(255) DEFAULT NULL COMMENT '描述：[0,255]用于产品规格描述',
  PRIMARY KEY (`cart_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='购物车';
DROP TABLE IF EXISTS `score`;
CREATE TABLE `score` (
  `score_id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '评分ID：',
  `user_id` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '评分人：',
  `nickname` varchar(64) CHARACTER SET utf8 DEFAULT NULL COMMENT '昵称：',
  `score_num` double(5,2) unsigned NOT NULL DEFAULT '0.00' COMMENT '评分：',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间：',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间：',
  `source_table` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '来源表：',
  `source_field` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '来源字段：',
  `source_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '来源ID：',
  PRIMARY KEY (`score_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='评分';
insert into `score` values ('1','0',null,'5.0','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','article','article_id','6');
insert into `score` values ('2','0',null,'3.0','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','article','article_id','6');
insert into `score` values ('3','0',null,'3.5','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','news','news_id','1');
insert into `score` values ('4','0',null,'100.0','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','news','news_id','2');
DROP TABLE IF EXISTS `order`;
CREATE TABLE `order` (
  `order_id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '订单ID：',
  `order_number` varchar(64) CHARACTER SET utf8 DEFAULT NULL COMMENT '订单号：',
  `goods_id` mediumint(8) unsigned NOT NULL COMMENT '商品id：[0,8388607]',
  `title` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '商品标题：',
  `img` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '商品图片：',
  `price` double(10,2) unsigned NOT NULL DEFAULT '0.00' COMMENT '价格：',
  `price_ago` double(10,2) unsigned NOT NULL DEFAULT '0.00' COMMENT '原价：',
  `num` int(8) NOT NULL DEFAULT '1' COMMENT '数量：',
  `price_count` double(8,2) unsigned NOT NULL DEFAULT '0.00' COMMENT '总价：',
  `norms` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '规格：',
  `type` varchar(64) NOT NULL DEFAULT '未分类' COMMENT '商品分类：',
  `contact_name` varchar(32) CHARACTER SET utf8 DEFAULT NULL COMMENT '联系人姓名：',
  `contact_email` varchar(125) CHARACTER SET utf8 DEFAULT NULL COMMENT '联系人邮箱：',
  `contact_phone` varchar(11) CHARACTER SET utf8 DEFAULT NULL COMMENT '联系人手机：',
  `contact_address` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '收件地址：',
  `postal_code` varchar(9) CHARACTER SET utf8 DEFAULT NULL COMMENT '邮政编码：',
  `user_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '买家ID：',
  `merchant_id` mediumint(8) unsigned NOT NULL DEFAULT '0' COMMENT '商家ID：',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间：',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间：',
  `description` varchar(255) DEFAULT NULL COMMENT '描述：[0,255]用于产品规格描述',
  `state` varchar(16) CHARACTER SET utf8 NOT NULL DEFAULT '待付款' COMMENT '订单状态：待付款，待发货，待签收，已签收，待退款，已退款，已拒绝，已完成',
  `remark` text CHARACTER SET utf8 COMMENT '订单备注',
  `delivery_state` varchar(16) CHARACTER SET utf8 DEFAULT '未配送' COMMENT '发货状态：未配送，已配送',
  `vip_discount` double(10,2) DEFAULT '0.00' COMMENT '折扣',
  `integral` int(11) DEFAULT '0' COMMENT '积分',
  `buy_type` tinyint(2) DEFAULT NULL COMMENT '1-全额购买，2-积分兑换，3-钱包购买',
  PRIMARY KEY (`order_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='订单';
DROP TABLE IF EXISTS `user_group`;
CREATE TABLE `user_group` (
  `group_id` mediumint(8) unsigned NOT NULL AUTO_INCREMENT COMMENT '用户组ID：[0,8388607]',
  `display` smallint(4) unsigned NOT NULL DEFAULT '100' COMMENT '显示顺序：[0,1000]',
  `name` varchar(16) CHARACTER SET utf8 NOT NULL DEFAULT '' COMMENT '名称：[0,16]',
  `description` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '描述：[0,255]描述该用户组的特点或权限范围',
  `source_table` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '来源表：',
  `source_field` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '来源字段：',
  `source_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '来源ID：',
  `register` smallint(1) unsigned DEFAULT '0' COMMENT '注册位置:',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间：',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间：',
  PRIMARY KEY (`group_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='用户组：用于用户前端身份和鉴权';
DROP TABLE IF EXISTS `article`;
CREATE TABLE `article` (
  `article_id` mediumint(8) unsigned NOT NULL AUTO_INCREMENT COMMENT '文章id：[0,8388607]',
  `title` varchar(125) NOT NULL DEFAULT '' COMMENT '标题：[0,125]用于文章和html的title标签中',
  `type` varchar(64) NOT NULL DEFAULT '0' COMMENT '文章分类：[0,1000]用来搜索指定类型的文章',
  `hits` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '点击数：[0,1000000000]访问这篇文章的人次',
  `praise_len` int(11) NOT NULL DEFAULT '0' COMMENT '点赞数',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间：',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间：',
  `source` varchar(255) DEFAULT NULL COMMENT '来源：[0,255]文章的出处',
  `url` varchar(255) DEFAULT NULL COMMENT '来源地址：[0,255]用于跳转到发布该文章的网站',
  `tag` varchar(255) DEFAULT NULL COMMENT '标签：[0,255]用于标注文章所属相关内容，多个标签用空格隔开',
  `content` longtext COMMENT '正文：文章的主体内容',
  `img` varchar(255) DEFAULT NULL COMMENT '封面图',
  `description` text COMMENT '文章描述',
  PRIMARY KEY (`article_id`,`title`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='文章：用于内容管理系统的文章';
DROP TABLE IF EXISTS `access_token`;
CREATE TABLE `access_token` (
  `token_id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '临时访问牌ID',
  `token` varchar(64) CHARACTER SET utf8 DEFAULT NULL COMMENT '临时访问牌',
  `info` text CHARACTER SET utf8,
  `maxage` int(2) NOT NULL DEFAULT '2' COMMENT '最大寿命：默认2小时',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间：',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间：',
  `user_id` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '用户编号:',
  PRIMARY KEY (`token_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='登陆访问时长';
insert into `access_token` values ('57','5accf85cb6a7f06f0aa2968deadaec1b',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('58','46ff1d4d07714f046ba07b34bffe0af9',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('59','ed9d6cba9826fda1beafcd9326be7a86',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('60','c99763c1833ea0785d9e2b81da3fd28f',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('61','33fbfaccd6d1cb9143e4129bd919d4b0',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('62','493e13da5f293ba67a56a0fe3e1fa6cf',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('63','c4b48e9e2160db09c703041a8fee0a1f',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('64','d13cdaefd3823c360c959a02a262f71d',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('65','6c6ff426fd77ea5a2025ce5ed2e42c8a',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('66','80930065a61ffcdd5cbb75f60932973c',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('67','94114763cf2e3b020495d8a27096d4ef',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('68','761052c551c97c9317bc3aa475c85b84',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('69','7c44ef14131a0ba7c16aa16cef104065',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('70','96380f3d9542c80d04bdade1cf7635a5',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('71','bfdc7acfcbf5763fda81945b60961222',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('72','170a598e51ae8ae2badde20a42fe171d',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('73','c82c357488c75926a92d8a9608d4b367',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('74','4d35290c023f407a820f37dbbb1ceb09',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('75','8d19162776682b695c0f62f3c7a92fec',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('76','a7ea2cdc9a2be179e19200e593ad5a69',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('77','c79a554f9832adc01f19682c5d576bc4',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('78','1c7d95001fa09951a679841c8100ad1f',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('79','776da1bcdd01ddb3cbf0a37fa13fc5b0',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('80','d336e88e57c329d0166931292c1fac41',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('81','37a40f526a6c82fc6110b512802d35bf',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('82','691ad331771f4109206d58aeee572371',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('83','9942e458886219960d3344b4a6a6fbec',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('84','e9939a8b7ccf9f548f0bbb5664981f96',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('85','f5b27912060d1909bef61fab9d96faae',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('86','7c5888682f1d449eb1b62f0054a79fbf',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('87','00dfdc6ac21c4a9da80fd71c990764d1',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('88','3cce592bc72840ab932ce96d85a194da',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('89','43fdaa989a644ad683ef4b4d488e8629',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('90','d6a3cecadacff0dbd6b43b25372cc2a2',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('91','5570bc5b07b3589f4ef8553bd46eb0d1',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('92','5570bc5b07b3589f4ef8553bd46eb0d1',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('93','26c553bd2ee2ab6605d18dfd310d85f9',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('94','3fd52f81236ed2c37ff91a6696d4e47a',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('95','893332e9ee67d60d8312b3700c58a359',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('96','b7844068ade535b2e517df4a40948703',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('97','179b37a5e1893c3af6b946bd5a1c8625',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('98','3a47b8a040a83ebbc9194cb255dc668c',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('99','afa60196afb77dcc2b520ed13a817560',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('100','7fc6d9b324f8c0a3a1784d04ef132692',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
insert into `access_token` values ('101','84e31b291f2bde6b7ceb27af5fe8eee3',null,'2','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0','1');
DROP TABLE IF EXISTS `code_token`;
CREATE TABLE `code_token` (
  `code_token_id` int(11) NOT NULL AUTO_INCREMENT,
  `token` varchar(255) CHARACTER SET latin1 DEFAULT NULL,
  `code` varchar(255) CHARACTER SET latin1 DEFAULT NULL COMMENT '验证码',
  `expire_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '失效时间',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间：',
  PRIMARY KEY (`code_token_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT;
DROP TABLE IF EXISTS `collect`;
CREATE TABLE `collect` (
  `collect_id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '收藏ID：',
  `user_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '收藏人ID：',
  `source_table` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '来源表：',
  `source_field` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '来源字段：',
  `source_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '来源ID：',
  `title` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '标题：',
  `img` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '封面：',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间：',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间：',
  PRIMARY KEY (`collect_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='收藏';
DROP TABLE IF EXISTS `notice`;
CREATE TABLE `notice` (
  `notice_id` mediumint(8) unsigned NOT NULL AUTO_INCREMENT COMMENT '公告id：',
  `title` varchar(125) CHARACTER SET utf8 NOT NULL DEFAULT '' COMMENT '标题：',
  `content` longtext CHARACTER SET utf8 COMMENT '正文：',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间：',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间：',
  PRIMARY KEY (`notice_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='公告';
insert into `notice` values ('1','网站公告','<p>公告，是指政府、团体对重大事件当众正式公布或者公开宣告，宣布。国务院2012年4月16日发布、2012年7月1日起施行的《党政机关公文处理工作条例》，对公告的使用表述为：“适用于向国内外宣布重要事项或者法定事项”。其中包含两方面的内容：一是向国内外宣布重要事项，公布依据政策、法令采取的重大行动等；二是向国内外宣布法定事项，公布依据法律规定告知国内外的有关重要规定和重大行动等。</p>','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0');
insert into `notice` values ('2','关于我们','<p>       一个网站要取得成功，要有先进的理念、先进的思想，更为重要的是抢占先机，及时行动。网络世界可谓一日千里、 日新月异，一个网站只有把握先机，抓住机遇，才</p><p>可能有更多的机会获得成功，可能处于网络行业发展的致高点，可能创建出成功的网站，才能能获得成功。要知道一种网站新模式在网络上只有保持几天的优势,因为人们很容易“COPY" 和模仿，因此，唯有不</p><p>断创新，不失时机地推出新的服务、新的模式、新的思想，网站才可能长久立于不败之地。</p>','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0');
insert into `notice` values ('3','联系方式','<h3>网站内容及品牌合作</h3><p>Email：xxxx@qq.com</p><h3>商务合作</h3><p>电话：010-xxxxxxx</p><p>Email：xxxx@qq.com</p><h3><br></h3><h3><br></h3><p><br></p>','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0');
insert into `notice` values ('4','网站介绍','<p>此处可上传文字、图片、视频、超链接、表格等内容区</p>','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0');
DROP TABLE IF EXISTS `address`;
CREATE TABLE `address` (
  `address_id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '收货地址：',
  `name` varchar(32) CHARACTER SET utf8 DEFAULT NULL COMMENT '姓名：',
  `phone` varchar(13) CHARACTER SET utf8 DEFAULT NULL COMMENT '手机：',
  `postcode` varchar(8) CHARACTER SET utf8 DEFAULT NULL COMMENT '邮编：',
  `address` varchar(255) CHARACTER SET utf8 NOT NULL COMMENT '地址：',
  `user_id` mediumint(8) unsigned NOT NULL COMMENT '用户ID：[0,8388607]用户获取其他与用户相关的数据',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间：',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间：',
  `default` tinyint(1) NOT NULL DEFAULT '0' COMMENT '默认判断',
  PRIMARY KEY (`address_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='收货地址：';
insert into `address` values ('3','田七','13590509090',null,'广州市越秀区东风西路187号','1','2024-12-31 18:21:49.0','2024-12-31 18:21:49.0',1);
DROP TABLE IF EXISTS `article_type`;
CREATE TABLE `article_type` (
  `type_id` smallint(5) unsigned NOT NULL AUTO_INCREMENT COMMENT '分类ID：[0,10000]',
  `display` smallint(4) unsigned NOT NULL DEFAULT '100' COMMENT '显示顺序：[0,1000]决定分类显示的先后顺序',
  `name` varchar(16) NOT NULL DEFAULT '' COMMENT '分类名称：[2,16]',
  `father_id` smallint(5) unsigned NOT NULL DEFAULT '0' COMMENT '上级分类ID：[0,32767]',
  `description` varchar(255) DEFAULT NULL COMMENT '描述：[0,255]描述该分类的作用',
  `icon` text COMMENT '分类图标：',
  `url` varchar(255) DEFAULT NULL COMMENT '外链地址：[0,255]如果该分类是跳转到其他网站的情况下，就在该URL上设置',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间：',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间：',
  PRIMARY KEY (`type_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='文章分类';
DROP TABLE IF EXISTS `praise`;
CREATE TABLE `praise` (
  `praise_id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '点赞ID：',
  `user_id` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '点赞人：',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间：',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间：',
  `source_table` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '来源表：',
  `source_field` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '来源字段：',
  `source_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '来源ID：',
  `status` tinyint(1) NOT NULL DEFAULT '1' COMMENT '点赞状态:1为点赞，0已取消',
  PRIMARY KEY (`praise_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='点赞';
DROP TABLE IF EXISTS `hits`;
CREATE TABLE `hits` (
  `hits_id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '点赞ID：',
  `user_id` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '点赞人：',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间：',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间：',
  `source_table` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '来源表：',
  `source_field` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '来源字段：',
  `source_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '来源ID：',
  PRIMARY KEY (`hits_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='用户点击';
DROP TABLE IF EXISTS `comment`;
CREATE TABLE `comment` (
  `comment_id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '评论ID：',
  `user_id` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '评论人ID：',
  `reply_to_id` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '回复评论ID：空为0',
  `content` longtext CHARACTER SET utf8 COMMENT '内容：',
  `nickname` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '昵称：',
  `avatar` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '头像地址：[0,255]',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间：',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间：',
  `source_table` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '来源表：',
  `source_field` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '来源字段：',
  `source_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '来源ID：',
  PRIMARY KEY (`comment_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='评论';
DROP TABLE IF EXISTS `goods_type`;
CREATE TABLE `goods_type` (
  `type_id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '商品分类ID：',
  `father_id` smallint(5) unsigned NOT NULL DEFAULT '0' COMMENT '上级分类ID：[0,32767]',
  `name` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '商品名称：',
  `desc` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '描述：',
  `icon` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '图标：',
  `source_table` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '来源表：',
  `source_field` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '来源字段：',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间：',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间：',
  PRIMARY KEY (`type_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='商品类型';
insert into `goods_type` values ('1','0','分类一','','',null,null,'2024-12-31 18:21:49.0','2024-12-31 18:21:49.0');
insert into `goods_type` values ('2','0','分类二','','',null,null,'2024-12-31 18:21:49.0','2024-12-31 18:21:49.0');
insert into `goods_type` values ('3','0','分类三','','',null,null,'2024-12-31 18:21:49.0','2024-12-31 18:21:49.0');
DROP TABLE IF EXISTS `user`;
CREATE TABLE `user` (
  `user_id` int(8) unsigned NOT NULL AUTO_INCREMENT COMMENT '用户ID：[0,8388607]用户获取其他与用户相关的数据',
  `state` smallint(1) unsigned NOT NULL DEFAULT '1' COMMENT '账户状态：[0,10](1可用|2异常|3已冻结|4已注销)',
  `user_group` varchar(32) CHARACTER SET utf8 DEFAULT NULL COMMENT '所在用户组：[0,32767]决定用户身份和权限',
  `login_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '上次登录时间：',
  `phone` varchar(11) CHARACTER SET utf8 DEFAULT NULL COMMENT '手机号码：[0,11]用户的手机号码，用于找回密码时或登录时',
  `phone_state` smallint(1) unsigned NOT NULL DEFAULT '0' COMMENT '手机认证：[0,1](0未认证|1审核中|2已认证)',
  `username` varchar(16) CHARACTER SET utf8 NOT NULL DEFAULT '' COMMENT '用户名：[0,16]用户登录时所用的账户名称',
  `nickname` varchar(16) CHARACTER SET utf8 DEFAULT '' COMMENT '昵称：[0,16]',
  `password` varchar(64) CHARACTER SET utf8 NOT NULL DEFAULT '' COMMENT '密码：[0,32]用户登录所需的密码，由6-16位数字或英文组成',
  `email` varchar(64) CHARACTER SET utf8 DEFAULT '' COMMENT '邮箱：[0,64]用户的邮箱，用于找回密码时或登录时',
  `email_state` smallint(1) unsigned NOT NULL DEFAULT '0' COMMENT '邮箱认证：[0,1](0未认证|1审核中|2已认证)',
  `avatar` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '头像地址：[0,255]',
  `open_id` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '针对获取用户信息字段',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间：',
  `vip_level` varchar(255) CHARACTER SET utf8 DEFAULT NULL COMMENT '会员等级',
  `vip_discount` double(10,2) DEFAULT '0.00' COMMENT '会员折扣',
  `integral` int(11) DEFAULT '0' COMMENT '积分',
  `balance` double(10,2) DEFAULT '0.00' COMMENT '余额',
  PRIMARY KEY (`user_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='用户账户：用于保存用户登录信息';
insert into `user` values ('1','1','管理员','2024-11-10 14:39:48.0',null,'0','admin','admin','bfd59291e825b5f2bbf1eb76569f8fe7','','0','/api/upload/admin_avatar.jpg',null,'2024-12-31 17:35:13.0',null,'0.0','0','0.0');
ALTER TABLE `user` DROP COLUMN balance;
ALTER TABLE `user` DROP COLUMN integral;
ALTER TABLE `user` DROP COLUMN vip_level;
ALTER TABLE `user` DROP COLUMN vip_discount;
ALTER TABLE `order` DROP COLUMN integral;
ALTER TABLE `order` DROP COLUMN buy_type;
ALTER TABLE `goods` DROP COLUMN integral;
UPDATE user SET avatar = '/api/upload/admin_avatar.jpg';
DROP TABLE IF EXISTS `ordinary_users`;
CREATE TABLE `ordinary_users`(ordinary_users_id int(11) NOT NULL AUTO_INCREMENT COMMENT '普通用户ID',
`user_name` varchar(64) comment '用户姓名',
`user_age` varchar(64) comment '用户年龄',
`user_gender` varchar(64) comment '用户性别',
`examine_state` varchar(16) DEFAULT '已通过' NOT NULL comment '审核状态',
`user_id` int(11) DEFAULT '0' NOT NULL comment '用户ID',
`create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
PRIMARY KEY (ordinary_users_id))ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 comment '普通用户';
insert into `ordinary_users` values (1,'用户姓名1','用户年龄1','用户性别1','已通过',2,'2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into user(user_group,username,nickname,password,avatar) values ('普通用户','yonghu1','yonghu1','bfd59291e825b5f2bbf1eb76569f8fe7','/api/upload/default_avatar.jpg');
insert into `ordinary_users` values (2,'用户姓名2','用户年龄2','用户性别2','已通过',3,'2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into user(user_group,username,nickname,password,avatar) values ('普通用户','yonghu2','yonghu2','bfd59291e825b5f2bbf1eb76569f8fe7','/api/upload/default_avatar.jpg');
insert into `ordinary_users` values (3,'用户姓名3','用户年龄3','用户性别3','已通过',4,'2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into user(user_group,username,nickname,password,avatar) values ('普通用户','yonghu3','yonghu3','bfd59291e825b5f2bbf1eb76569f8fe7','/api/upload/default_avatar.jpg');

DROP TABLE IF EXISTS `inheriting_users`;
CREATE TABLE `inheriting_users`(inheriting_users_id int(11) NOT NULL AUTO_INCREMENT COMMENT '传承用户ID',
`personnel_name` varchar(64) comment '人员姓名',
`age_of_personnel` varchar(64) comment '人员年龄',
`gender_of_personnel` varchar(64) comment '人员性别',
`home_address` varchar(64) comment '家庭住址',
`id_picture` varchar(255) comment '证件图片',
`examine_state` varchar(16) DEFAULT '未审核' NOT NULL comment '审核状态',
`user_id` int(11) DEFAULT '0' NOT NULL comment '用户ID',
`create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
PRIMARY KEY (inheriting_users_id))ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 comment '传承用户';
insert into `inheriting_users` values (1,'人员姓名1','人员年龄1','人员性别1','家庭住址1','','已通过',5,'2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into user(user_group,username,nickname,password,avatar) values ('传承用户','yonghu4','yonghu4','bfd59291e825b5f2bbf1eb76569f8fe7','/api/upload/default_avatar.jpg');
insert into `inheriting_users` values (2,'人员姓名2','人员年龄2','人员性别2','家庭住址2','','已通过',6,'2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into user(user_group,username,nickname,password,avatar) values ('传承用户','yonghu5','yonghu5','bfd59291e825b5f2bbf1eb76569f8fe7','/api/upload/default_avatar.jpg');
insert into `inheriting_users` values (3,'人员姓名3','人员年龄3','人员性别3','家庭住址3','','已通过',7,'2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into user(user_group,username,nickname,password,avatar) values ('传承用户','yonghu6','yonghu6','bfd59291e825b5f2bbf1eb76569f8fe7','/api/upload/default_avatar.jpg');

DROP TABLE IF EXISTS `cultural_and_creative_mall`;
CREATE TABLE `cultural_and_creative_mall`(cultural_and_creative_mall_id int(11) NOT NULL AUTO_INCREMENT COMMENT '文创商城ID',
`seller_customers` int(11) DEFAULT 0 comment '卖家用户',
`personnel_name` varchar(64) comment '人员姓名',
`product_specifications` varchar(64) comment '商品规格',
`hits` int(11) DEFAULT 0 NOT NULL comment '点击数',
`praise_len` int(11) DEFAULT 0 NOT NULL comment '点赞数',
`cart_title` varchar(125) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '标题：[0,125]用于产品html的标签中',
`cart_img` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL COMMENT '封面图：用于显示于产品列表页',
`cart_description` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '描述：[0,255]用于产品规格描述',
`cart_price_ago` double(8, 2) UNSIGNED NOT NULL DEFAULT 0.00 COMMENT '原价：[1]',
`cart_price` double(8, 2) UNSIGNED NOT NULL DEFAULT 0.00 COMMENT '卖价：[1]',
`cart_inventory` int(11) NOT NULL DEFAULT 0 COMMENT '商品库存',
`cart_type` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '未分类' COMMENT '商品分类：',
`cart_content` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL COMMENT '正文：产品的主体内容',
`cart_img_1` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL COMMENT '主图1：',
`cart_img_2` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL COMMENT '主图2：',
`cart_img_3` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL COMMENT '主图3：',
`cart_img_4` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL COMMENT '主图4：',
`cart_img_5` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL COMMENT '主图5：',
`create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
PRIMARY KEY (cultural_and_creative_mall_id))ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 comment '文创商城';
insert into `cultural_and_creative_mall` values (1,0,'人员姓名1','商品规格1',487,411,'测试商品1','/upload/goods.jpg','测试商品1','110.00','99.00','80','分类一','<p>测试商品1</p>','/upload/img_1.jpg','/upload/img_2.jpg','/upload/img_3.jpg','/upload/img_4.jpg','/upload/img_5.jpg','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `cultural_and_creative_mall` values (2,0,'人员姓名2','商品规格2',644,565,'测试商品2','/upload/goods.jpg','测试商品2','220.00','110.00','10','分类二','<p>测试商品2</p>','/upload/img_1.jpg','/upload/img_2.jpg','/upload/img_3.jpg','/upload/img_4.jpg','/upload/img_5.jpg','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `cultural_and_creative_mall` values (3,0,'人员姓名3','商品规格3',22,236,'测试商品3','/upload/goods.jpg','测试商品3','20.00','10.00','10','分类二','<p>测试商品3</p>','/upload/img_1.jpg','/upload/img_2.jpg','/upload/img_3.jpg','/upload/img_4.jpg','/upload/img_5.jpg','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `cultural_and_creative_mall` values (4,0,'人员姓名4','商品规格4',129,581,'测试商品4','/upload/goods.jpg','测试商品4','199.00','99.00','20','分类二','<p>测试商品4</p>','/upload/img_1.jpg','/upload/img_2.jpg','/upload/img_3.jpg','/upload/img_4.jpg','/upload/img_5.jpg','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `cultural_and_creative_mall` values (5,0,'人员姓名5','商品规格5',413,845,'测试商品5','/upload/goods.jpg','测试商品5','110.00','99.00','30','分类二','<p>测试商品5</p>','/upload/img_1.jpg','/upload/img_2.jpg','/upload/img_3.jpg','/upload/img_4.jpg','/upload/img_5.jpg','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `cultural_and_creative_mall` values (6,0,'人员姓名6','商品规格6',719,472,'测试商品6','/upload/goods.jpg','测试商品6','999.00','888.00','40','分类三','<p>测试商品6</p>','/upload/img_1.jpg','/upload/img_2.jpg','/upload/img_3.jpg','/upload/img_4.jpg','/upload/img_5.jpg','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `cultural_and_creative_mall` values (7,0,'人员姓名7','商品规格7',475,884,'测试商品7','/upload/goods.jpg','测试商品7','20.00','220.00','110','分类一','<p>测试商品7</p>','/upload/img_1.jpg','/upload/img_2.jpg','/upload/img_3.jpg','/upload/img_4.jpg','/upload/img_5.jpg','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `cultural_and_creative_mall` values (8,0,'人员姓名8','商品规格8',213,876,'测试商品8','/upload/goods.jpg','测试商品8','20.00','30.00','30','分类二','<p>测试商品8</p>','/upload/img_1.jpg','/upload/img_2.jpg','/upload/img_3.jpg','/upload/img_4.jpg','/upload/img_5.jpg','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `cultural_and_creative_mall` values (9,0,'人员姓名9','商品规格9',698,681,'测试商品9','/upload/goods.jpg','测试商品9','40.00','35.00','20','分类三','<p>测试商品9</p>','/upload/img_1.jpg','/upload/img_2.jpg','/upload/img_3.jpg','/upload/img_4.jpg','/upload/img_5.jpg','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `cultural_and_creative_mall` values (10,0,'人员姓名10','商品规格10',532,99,'测试商品10','/upload/goods.jpg','测试商品10','50.00','45.00','80','分类一','<p>测试商品10</p>','/upload/img_1.jpg','/upload/img_2.jpg','/upload/img_3.jpg','/upload/img_4.jpg','/upload/img_5.jpg','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `cultural_and_creative_mall` values (11,0,'人员姓名11','商品规格11',414,436,'测试商品11','/upload/goods.jpg','测试商品11','60.00','50.00','10','分类二','<p>测试商品11</p>','/upload/img_1.jpg','/upload/img_2.jpg','/upload/img_3.jpg','/upload/img_4.jpg','/upload/img_5.jpg','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `cultural_and_creative_mall` values (12,0,'人员姓名12','商品规格12',967,735,'测试商品12','/upload/goods.jpg','测试商品12','70.00','60.00','30','分类三','<p>测试商品12</p>','/upload/img_1.jpg','/upload/img_2.jpg','/upload/img_3.jpg','/upload/img_4.jpg','/upload/img_5.jpg','2024-12-02 14:49:15','2024-12-02 14:49:15');

update `goods` set customize_field = '[{"field_name":"卖家用户","field_value":0},{"field_name":"人员姓名","field_value":"人员姓名1"},{"field_name":"商品规格","field_value":"商品规格1"}]' , source_table = 'cultural_and_creative_mall' , source_field = 'cultural_and_creative_mall_id' , source_id = 1 where goods_id = 1;
update `goods` set customize_field = '[{"field_name":"卖家用户","field_value":0},{"field_name":"人员姓名","field_value":"人员姓名2"},{"field_name":"商品规格","field_value":"商品规格2"}]' , source_table = 'cultural_and_creative_mall' , source_field = 'cultural_and_creative_mall_id' , source_id = 2 where goods_id = 2;
update `goods` set customize_field = '[{"field_name":"卖家用户","field_value":0},{"field_name":"人员姓名","field_value":"人员姓名3"},{"field_name":"商品规格","field_value":"商品规格3"}]' , source_table = 'cultural_and_creative_mall' , source_field = 'cultural_and_creative_mall_id' , source_id = 3 where goods_id = 3;
update `goods` set customize_field = '[{"field_name":"卖家用户","field_value":0},{"field_name":"人员姓名","field_value":"人员姓名4"},{"field_name":"商品规格","field_value":"商品规格4"}]' , source_table = 'cultural_and_creative_mall' , source_field = 'cultural_and_creative_mall_id' , source_id = 4 where goods_id = 4;
update `goods` set customize_field = '[{"field_name":"卖家用户","field_value":0},{"field_name":"人员姓名","field_value":"人员姓名5"},{"field_name":"商品规格","field_value":"商品规格5"}]' , source_table = 'cultural_and_creative_mall' , source_field = 'cultural_and_creative_mall_id' , source_id = 5 where goods_id = 5;
update `goods` set customize_field = '[{"field_name":"卖家用户","field_value":0},{"field_name":"人员姓名","field_value":"人员姓名6"},{"field_name":"商品规格","field_value":"商品规格6"}]' , source_table = 'cultural_and_creative_mall' , source_field = 'cultural_and_creative_mall_id' , source_id = 6 where goods_id = 6;
update `goods` set customize_field = '[{"field_name":"卖家用户","field_value":0},{"field_name":"人员姓名","field_value":"人员姓名7"},{"field_name":"商品规格","field_value":"商品规格7"}]' , source_table = 'cultural_and_creative_mall' , source_field = 'cultural_and_creative_mall_id' , source_id = 7 where goods_id = 7;
update `goods` set customize_field = '[{"field_name":"卖家用户","field_value":0},{"field_name":"人员姓名","field_value":"人员姓名8"},{"field_name":"商品规格","field_value":"商品规格8"}]' , source_table = 'cultural_and_creative_mall' , source_field = 'cultural_and_creative_mall_id' , source_id = 8 where goods_id = 8;
update `goods` set customize_field = '[{"field_name":"卖家用户","field_value":0},{"field_name":"人员姓名","field_value":"人员姓名9"},{"field_name":"商品规格","field_value":"商品规格9"}]' , source_table = 'cultural_and_creative_mall' , source_field = 'cultural_and_creative_mall_id' , source_id = 9 where goods_id = 9;
update `goods` set customize_field = '[{"field_name":"卖家用户","field_value":0},{"field_name":"人员姓名","field_value":"人员姓名10"},{"field_name":"商品规格","field_value":"商品规格10"}]' , source_table = 'cultural_and_creative_mall' , source_field = 'cultural_and_creative_mall_id' , source_id = 10 where goods_id = 10;
update `goods` set customize_field = '[{"field_name":"卖家用户","field_value":0},{"field_name":"人员姓名","field_value":"人员姓名11"},{"field_name":"商品规格","field_value":"商品规格11"}]' , source_table = 'cultural_and_creative_mall' , source_field = 'cultural_and_creative_mall_id' , source_id = 11 where goods_id = 11;
update `goods` set customize_field = '[{"field_name":"卖家用户","field_value":0},{"field_name":"人员姓名","field_value":"人员姓名12"},{"field_name":"商品规格","field_value":"商品规格12"}]' , source_table = 'cultural_and_creative_mall' , source_field = 'cultural_and_creative_mall_id' , source_id = 12 where goods_id = 12;
DROP TABLE IF EXISTS `project_classification`;
CREATE TABLE `project_classification`(project_classification_id int(11) NOT NULL AUTO_INCREMENT COMMENT '项目分类ID',
`project_category` varchar(64) comment '项目类别',
`create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
PRIMARY KEY (project_classification_id))ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 comment '项目分类';
insert into `project_classification` values (1,'项目类别1','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `project_classification` values (2,'项目类别2','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `project_classification` values (3,'项目类别3','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `project_classification` values (4,'项目类别4','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `project_classification` values (5,'项目类别5','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `project_classification` values (6,'项目类别6','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `project_classification` values (7,'项目类别7','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `project_classification` values (8,'项目类别8','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `project_classification` values (9,'项目类别9','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `project_classification` values (10,'项目类别10','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `project_classification` values (11,'项目类别11','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `project_classification` values (12,'项目类别12','2024-12-02 14:49:15','2024-12-02 14:49:15');

DROP TABLE IF EXISTS `regional_classification`;
CREATE TABLE `regional_classification`(regional_classification_id int(11) NOT NULL AUTO_INCREMENT COMMENT '地区分类ID',
`regional_name` varchar(64) comment '地区名称',
`create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
PRIMARY KEY (regional_classification_id))ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 comment '地区分类';
insert into `regional_classification` values (1,'地区名称1','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `regional_classification` values (2,'地区名称2','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `regional_classification` values (3,'地区名称3','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `regional_classification` values (4,'地区名称4','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `regional_classification` values (5,'地区名称5','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `regional_classification` values (6,'地区名称6','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `regional_classification` values (7,'地区名称7','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `regional_classification` values (8,'地区名称8','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `regional_classification` values (9,'地区名称9','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `regional_classification` values (10,'地区名称10','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `regional_classification` values (11,'地区名称11','2024-12-02 14:49:15','2024-12-02 14:49:15');
insert into `regional_classification` values (12,'地区名称12','2024-12-02 14:49:15','2024-12-02 14:49:15');

DROP TABLE IF EXISTS `intangible_cultural_heritage_projects`;
CREATE TABLE `intangible_cultural_heritage_projects`(intangible_cultural_heritage_projects_id int(11) NOT NULL AUTO_INCREMENT COMMENT '非遗项目ID',
`entry_name` varchar(64) comment '项目名称',
`regional_name` varchar(64) comment '地区名称',
`project_category` varchar(64) comment '项目类别',
`cover_photo` varchar(255) comment '封面图片',
`inheritance_personnel` int(11) DEFAULT 0 comment '传承人员',
`personnel_name` varchar(64) comment '人员姓名',
`announcement_date` date comment '公布日期',
`declaration_unit` varchar(64) comment '申报单位',
`protecting_units` varchar(64) comment '保护单位',
`detailed_introduction` text comment '详细介绍',
`hits` int(11) DEFAULT 0 NOT NULL comment '点击数',
`praise_len` int(11) DEFAULT 0 NOT NULL comment '点赞数',
`create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
PRIMARY KEY (intangible_cultural_heritage_projects_id))ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 comment '非遗项目';
insert into `intangible_cultural_heritage_projects` values (1,'项目名称1','地区名称1','项目类别1','/api/upload/1834538850668511233.jpg',0,'人员姓名1','2024-12-02','申报单位1','保护单位1','详细介绍1',314,696,'2024-12-02 14:49:16','2024-12-02 14:49:16');
insert into `intangible_cultural_heritage_projects` values (2,'项目名称2','地区名称2','项目类别2','/api/upload/1834538676592312320.jpg',0,'人员姓名2','2024-12-02','申报单位2','保护单位2','详细介绍2',632,558,'2024-12-02 14:49:16','2024-12-02 14:49:16');
insert into `intangible_cultural_heritage_projects` values (3,'项目名称3','地区名称3','项目类别3','/api/upload/1834538727997702145.jpg',0,'人员姓名3','2024-12-02','申报单位3','保护单位3','详细介绍3',41,458,'2024-12-02 14:49:16','2024-12-02 14:49:16');
insert into `intangible_cultural_heritage_projects` values (4,'项目名称4','地区名称4','项目类别4','/api/upload/1834538768695033856.jpg',0,'人员姓名4','2024-12-02','申报单位4','保护单位4','详细介绍4',425,725,'2024-12-02 14:49:16','2024-12-02 14:49:16');
insert into `intangible_cultural_heritage_projects` values (5,'项目名称5','地区名称5','项目类别5','/api/upload/1834539060425654272.jpg',0,'人员姓名5','2024-12-02','申报单位5','保护单位5','详细介绍5',554,597,'2024-12-02 14:49:16','2024-12-02 14:49:16');
insert into `intangible_cultural_heritage_projects` values (6,'项目名称6','地区名称6','项目类别6','/api/upload/1834538591707987968.jpg',0,'人员姓名6','2024-12-02','申报单位6','保护单位6','详细介绍6',273,232,'2024-12-02 14:49:16','2024-12-02 14:49:16');
insert into `intangible_cultural_heritage_projects` values (7,'项目名称7','地区名称7','项目类别7','/api/upload/1834538633701359616.jpg',0,'人员姓名7','2024-12-02','申报单位7','保护单位7','详细介绍7',363,759,'2024-12-02 14:49:16','2024-12-02 14:49:16');
insert into `intangible_cultural_heritage_projects` values (8,'项目名称8','地区名称8','项目类别8','/api/upload/1834538809379782657.jpg',0,'人员姓名8','2024-12-02','申报单位8','保护单位8','详细介绍8',63,785,'2024-12-02 14:49:16','2024-12-02 14:49:16');
insert into `intangible_cultural_heritage_projects` values (9,'项目名称9','地区名称9','项目类别9','/api/upload/1834538964371898368.jpg',0,'人员姓名9','2024-12-02','申报单位9','保护单位9','详细介绍9',625,560,'2024-12-02 14:49:16','2024-12-02 14:49:16');
insert into `intangible_cultural_heritage_projects` values (10,'项目名称10','地区名称10','项目类别10','/api/upload/1834539014858735617.jpg',0,'人员姓名10','2024-12-02','申报单位10','保护单位10','详细介绍10',454,492,'2024-12-02 14:49:16','2024-12-02 14:49:16');
insert into `intangible_cultural_heritage_projects` values (11,'项目名称11','地区名称11','项目类别11','/api/upload/1834538899888668672.jpg',0,'人员姓名11','2024-12-02','申报单位11','保护单位11','详细介绍11',900,454,'2024-12-02 14:49:16','2024-12-02 14:49:16');
insert into `intangible_cultural_heritage_projects` values (12,'项目名称12','地区名称12','项目类别12','/api/upload/1834538550792552448.jpg',0,'人员姓名12','2024-12-02','申报单位12','保护单位12','详细介绍12',7,898,'2024-12-02 14:49:16','2024-12-02 14:49:16');

DROP TABLE IF EXISTS `inheritance_personnel`;
CREATE TABLE `inheritance_personnel`(inheritance_personnel_id int(11) NOT NULL AUTO_INCREMENT COMMENT '传承人员ID',
`entry_name` varchar(64) comment '项目名称',
`project_category` varchar(64) comment '项目类别',
`inheriting_users` int(11) DEFAULT 0 comment '传承用户',
`personnel_name` varchar(64) comment '人员姓名',
`age_of_personnel` varchar(64) comment '人员年龄',
`gender_of_personnel` varchar(64) comment '人员性别',
`cover_photo` varchar(255) comment '封面图片',
`date_of_birth` date comment '出生日期',
`declaration_region` varchar(64) comment '申报地区',
`personal_profile` text comment '个人简介',
`hits` int(11) DEFAULT 0 NOT NULL comment '点击数',
`praise_len` int(11) DEFAULT 0 NOT NULL comment '点赞数',
`create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
`update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
PRIMARY KEY (inheritance_personnel_id))ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 comment '传承人员';
insert into `inheritance_personnel` values (1,'项目名称1','项目类别1',0,'人员姓名1','人员年龄1','人员性别1','/api/upload/1837341784473075712.jpg','2024-12-02','申报地区1','个人简介1',858,443,'2024-12-02 14:49:16','2024-12-02 14:49:16');
insert into `inheritance_personnel` values (2,'项目名称2','项目类别2',0,'人员姓名2','人员年龄2','人员性别2','/api/upload/1837341769495216129.jpg','2024-12-02','申报地区2','个人简介2',447,203,'2024-12-02 14:49:16','2024-12-02 14:49:16');
insert into `inheritance_personnel` values (3,'项目名称3','项目类别3',0,'人员姓名3','人员年龄3','人员性别3','/api/upload/1837341913011716097.jpg','2024-12-02','申报地区3','个人简介3',772,387,'2024-12-02 14:49:16','2024-12-02 14:49:16');
insert into `inheritance_personnel` values (4,'项目名称4','项目类别4',0,'人员姓名4','人员年龄4','人员性别4','/api/upload/1837341858217328640.jpg','2024-12-02','申报地区4','个人简介4',801,882,'2024-12-02 14:49:16','2024-12-02 14:49:16');
insert into `inheritance_personnel` values (5,'项目名称5','项目类别5',0,'人员姓名5','人员年龄5','人员性别5','/api/upload/1837341934864039937.jpg','2024-12-02','申报地区5','个人简介5',105,726,'2024-12-02 14:49:16','2024-12-02 14:49:16');
insert into `inheritance_personnel` values (6,'项目名称6','项目类别6',0,'人员姓名6','人员年龄6','人员性别6','/api/upload/1837341957198708737.jpg','2024-12-02','申报地区6','个人简介6',436,801,'2024-12-02 14:49:16','2024-12-02 14:49:16');
insert into `inheritance_personnel` values (7,'项目名称7','项目类别7',0,'人员姓名7','人员年龄7','人员性别7','/api/upload/1837341842127978496.jpg','2024-12-02','申报地区7','个人简介7',905,839,'2024-12-02 14:49:16','2024-12-02 14:49:16');
insert into `inheritance_personnel` values (8,'项目名称8','项目类别8',0,'人员姓名8','人员年龄8','人员性别8','/api/upload/1837341824218300417.jpg','2024-12-02','申报地区8','个人简介8',298,538,'2024-12-02 14:49:16','2024-12-02 14:49:16');
insert into `inheritance_personnel` values (9,'项目名称9','项目类别9',0,'人员姓名9','人员年龄9','人员性别9','/api/upload/1837341874495422464.jpg','2024-12-02','申报地区9','个人简介9',704,344,'2024-12-02 14:49:16','2024-12-02 14:49:16');
insert into `inheritance_personnel` values (10,'项目名称10','项目类别10',0,'人员姓名10','人员年龄10','人员性别10','/api/upload/1837341807524970496.jpg','2024-12-02','申报地区10','个人简介10',874,607,'2024-12-02 14:49:16','2024-12-02 14:49:16');
insert into `inheritance_personnel` values (11,'项目名称11','项目类别11',0,'人员姓名11','人员年龄11','人员性别11','/api/upload/1837341893382373376.jpg','2024-12-02','申报地区11','个人简介11',763,93,'2024-12-02 14:49:16','2024-12-02 14:49:16');
insert into `inheritance_personnel` values (12,'项目名称12','项目类别12',0,'人员姓名12','人员年龄12','人员性别12','/api/upload/1837341743465365505.jpg','2024-12-02','申报地区12','个人简介12',903,413,'2024-12-02 14:49:16','2024-12-02 14:49:16');

insert into `user_group` values ('1','100','管理员',null,'','','0','0','2024-12-02 14:49:17.0','2024-12-02 14:49:17.0');
insert into `user_group` values ('2','100','游客',null,'','','0','0','2024-12-02 14:49:17.0','2024-12-02 14:49:17.0');
insert into `user_group` values ('3','100','普通用户',null,'ordinary_users','ordinary_users_id','0','3','2024-12-02 14:49:17.0','2024-12-02 14:49:17.0');
insert into `user_group` values ('4','100','传承用户',null,'inheriting_users','inheriting_users_id','0','3','2024-12-02 14:49:17.0','2024-12-02 14:49:17.0');
ALTER TABLE `comment` ADD FOREIGN KEY (`user_id`) REFERENCES `user` (`user_id`) ON DELETE CASCADE;
ALTER TABLE `collect` ADD FOREIGN KEY (`user_id`) REFERENCES `user` (`user_id`) ON DELETE CASCADE;
ALTER TABLE `hits` ADD FOREIGN KEY (`user_id`) REFERENCES `user` (`user_id`) ON DELETE CASCADE;
ALTER TABLE `praise` ADD FOREIGN KEY (`user_id`) REFERENCES `user` (`user_id`) ON DELETE CASCADE;
ALTER TABLE `cart` ADD FOREIGN KEY (`goods_id`) REFERENCES `goods` (`goods_id`) ON DELETE CASCADE;
ALTER TABLE `order` ADD FOREIGN KEY (`goods_id`) REFERENCES `goods` (`goods_id`) ON DELETE CASCADE;
insert into `slides` values ('1','轮播图1','内容1','/article/details?article=1','/api/upload/1834538486112190464.jpg','21','2024-12-02 14:49:17.0','2024-12-02 14:49:17.0');
insert into `slides` values ('2','轮播图2','内容2','/article/details?article=2','/api/upload/1834538427178024961.jpg','924','2024-12-02 14:49:17.0','2024-12-02 14:49:17.0');
insert into `slides` values ('3','轮播图3','内容3','/article/details?article=3','/api/upload/1834538350741028864.jpg','44','2024-12-02 14:49:17.0','2024-12-02 14:49:17.0');
insert into `article` values ('1','全省举办非遗惠民活动1300多场','非遗','632','0','2024-12-02 14:49:18.0','2024-12-02 14:49:18.0',null,null,null,'<p>记者从省文化和旅游厅获悉，国庆节期间，全省共举办各类非遗惠民活动1300多场，近1500万名群众参与，各类产品销售额达5000多万元。</p><p>河南非遗美学馆举办了“带着非遗去旅行——河南非遗美食国庆市集”及非遗体验惠民活动。7天时间共举办活动10多场，共计接待游客近万人次。</p><p class="ql-align-center"><span style="color: rgb(127, 127, 127);">市民观看非遗表演</span></p><p>郑州登封市精选登封窑、嵩山传拓、舞狮等23个非遗项目集中亮相国庆假期举办的“非遗市集”，为全国各地游客展现了登封“双遗之城”的独特魅力，吸引了众多游客体验、拍照。</p><p>开封市举办“开封非遗市集 国庆节专场”，近百个非遗项目向市民和游客展现非遗保护成果。洛阳市开展非遗相关活动共40余场，直接参与群众10万余人。平顶山举办河南（宝丰）民间唢呐展演，全省16支代表队先后登台展演，现场观众达3000人次。</p><p>安阳市打造“有文化 必安阳”2024安阳动感商潮非遗文创集市，吸引了众多游客参观体验。鹤壁市以非遗展示进景区、非遗产品集中秀等特色活动，以非遗集市、美食嘉年华等平台，激发文旅新活力。新乡市打造“石榴花开”新乡非遗主题场景省级夜间文旅消费集聚区，有75个非遗项目，15家老字号和众多手作匠人共同推出近1000种非遗文创产品。</p><p>焦作市举办焦作非物质文化遗产名录展，首次以“图片+文字+灯光+美陈”的形式对人类非遗项目太极拳、13个国家级项目及87个省级项目进行了全面展示。濮阳市举办了2024濮阳市非遗展会和2024濮阳市非遗剪纸百龙作品展，为广大群众带来一场场文化盛宴。</p><p>许昌禹州市在神垕古镇开展了“国庆非遗盛宴，点亮神垕老街”非遗市集活动，钧瓷烧制技艺等10余项非遗“点亮”了景区。漯河市举办“艺起同行，传承多彩非遗”非遗项目进景区活动。三门峡市陕州区张汴乡北营村，游客走进素有“民俗史上活化石”之称的地坑院，观赏锣鼓书、捶草印花、川剧变脸、天桥杂耍等，近距离感受非遗文化的独特韵味。</p><p>此外，南阳市举办“举国欢腾庆国庆 大国文明永传承——非遗皮影体验活动”，商丘市举办“豫剧文化艺术周”——千年古韵·绽豫剧华彩活动，信阳市开展非遗进景区活动，周口市共举办非遗传承实践活动12场，驻马店市组织非遗项目参与社科普及周活动。</p>','/api/upload/1844186251205279745.jpg',null);
insert into `article` values ('2','体验非遗魅力 传承民俗文化','非遗','429','0','2024-12-02 14:49:18.0','2024-12-02 14:49:18.0',null,null,null,'<p>青瓦白墙间，游客循着米糕米酒的香气，品尝非遗美食。悠扬婉转的伽倻琴声中，《印象延吉》传统歌舞表演拉开帷幕……这里是吉林省延边朝鲜族自治州中国朝鲜族民俗园。</p><p>近年来，延边州中国朝鲜族民俗园通过“旅游+民俗+体验”模式，推动民俗展示、非遗体验、特色餐饮、文化交流有机融合，让延边旅游因特色文化更添热度，让特色文化随旅游热度而广泛传播。</p><p>民俗园内的一家服饰店，或素雅端庄、或灵动飘逸的朝鲜族服饰琳琅满目，游客在服务人员的介绍下认真选购。</p><p>化妆间内，山西游客高焱换好了一身素白蓝纹长裙，端坐镜前，与化妆师沟通着妆容、头饰等细节。“旅拍、化妆服务贴心又专业，体验非常好!”高焱说。</p><p>朝鲜族民族服饰以斜襟、无纽扣等为特色，2008年入选了第二批国家级非物质文化遗产名录。“在保留特色的基础上，我们又进一步改良，融合了对襟、盘扣、挂钩等多种设计元素。”该服饰店负责人崔勋介绍。</p><p>“社交媒体上的旅拍照片，常是游客了解延边和我们民俗园的第一步。”中国朝鲜族民俗园运营总监朱保坤介绍，近年来园区着力打造特色旅拍品牌，建设集服装租赁、美妆、摄影于一体的旅拍基地。渐渐地，身着民族服装、拍照打卡成为不少游客来延边旅游的必选项。</p><p>“早些年，民俗园周边的旅拍店数量不超过5家。”朱保坤说，如今民俗园周边大大小小的旅拍店加起来有近千家，旺季时依然供不应求。</p><p>一栋栋飞檐翘角的传统民居错落有致，民俗园由40栋朝鲜族特色民居组成，其中9栋建筑已有百年历史。园区一方面持续做好传统民居保护修缮工作，另一方面将多项非物质文化遗产引入传统民居，为游客带来沉浸式的民俗文化体验。</p><p>走进“农夫之家”朝鲜族民居院落，朝鲜族爱玲米酒制作技艺第四代传承人安贞今放下电话，满面笑容。“闺女在家里酿米酒，刚在问我配料怎么放呢。”安贞今说，年轻人也在认真传承、钻研米酒制作技艺，让她非常欣慰。</p><p>安贞今告诉记者，进入园区经营近10年，她不断改良、创新传统技术，推出桑葚米酒、蓝莓米酒等新产品，深受游客喜爱，旺季时一天收入最高近万元。</p><p>走进陶艺工坊，朝鲜族民俗陶艺制作技艺传承人金英玉正对着烧制出的陶器细细描摹；在朝鲜族米糕非遗体验馆内，福袋、金达莱花等造型的米糕新鲜出炉，引得不少游客驻足品尝……</p><p>“我们利用老宅打造‘非遗文化展游区’，增进游客对朝鲜族民俗的了解，民俗文化也得到了活态传承。”朱保坤说。</p><p>民俗园内的演艺广场上，由延边大学艺术学院师生带来的朝鲜族歌舞表演《印象延吉》吸引游客驻足观赏。“演出太震撼了，让我对朝鲜族歌舞有了更多了解。”来自广东的游客张梓萱意犹未尽。</p><p>为推动朝鲜族文化元素与旅游深度融合，民俗园与延边大学等合作创排实景演出《印象延吉》，并在多功能剧场、喷泉广场等地进行扇子舞、长鼓舞、伽倻琴等表演。每逢节庆，民俗园内还会开展秋千、摔跤等朝鲜族特色非物质文化遗产的展演活动。</p><p>在园区内的传统手工艺品展示体验馆，游客们正在选购具有朝鲜族文化特色的钥匙扣、冰箱贴等文创产品。“通过文创产品，越来越多人对朝鲜族传统手工艺产生兴趣。”体验馆负责人朴正爱说，朝鲜族编结等传统手工艺品也会在这里展销，“手工艺人们有了更多展示空间。”</p><p>延边州文化广播电视和旅游局局长宋艳说，延边州将继续坚持以文塑旅、以旅彰文，提高服务品质、改善文化体验，让朝鲜族传统文化与旅游在融合中实现创新发展。</p>','/api/upload/1844186410895015936.jpg',null);
insert into `article` values ('3','皮影也科技——非遗科技文化展','非遗','90','0','2024-12-02 14:49:18.0','2024-12-02 14:49:18.0',null,null,null,'<p class="ql-align-justify">	近日，我市科技馆引进由中国科技馆和北京皮影剧团联合打造的“皮影也科技——非遗科技文化展”，展现非物质文化遗产在新时代的新风采，引导公众从科技视角认识非遗文化，促进公众科学文化素质的提升。</p><p class="ql-align-justify">	此次展览集非遗文化和科技体验于一体，从“皮影戏+科技”的角度出发，提炼出“影”“皮”“舞”“承”四个关键词构架展览内容，对应设置四个分展区，揭示皮影背后的光学、力学、材料学和心理学等科学原理，展示皮影的起源与变迁、传承与发展。展览面向公众免费开放，截至目前，观展人数超过1万人次。</p><p class="ql-align-justify">	依托展览，市科技馆开展主题讲解12场次，滁州市清流小学、滁州市银山路小学、滁州市永乐小学300余名师生先后参观展览，开展特色研学活动；同时，市科技馆充分利用高校科普教育资源，联合滁州学院机械与电气工程学院打造皮影体验创客工坊，通过“匠心传承”学皮影、“光影之舞”观皮影、“指尖古韵”做皮影、“翩舞弄影”演皮影、“筑梦留影”拍皮影等环节，由大学生志愿者讲解皮影相关知识以及背后的科学原理，指导学生亲手制作皮影，演绎皮影。</p><p class="ql-align-justify">	据了解，市科技馆围绕“提升全民科学素质 协力建设科技强国”主题，线上线下相结合，科普阵地相联合，积极开展全国科普日系列活动30余场次，受众1.6万人次，充分弘扬了科学精神和科学家精神，营造了良好的社会氛围。</p><p><br></p>','/api/upload/1844186671877193729.jpg',null);
insert into `article` values ('4','透过“物质”看“非遗”','非遗','55','0','2024-12-02 14:49:18.0','2024-12-02 14:49:18.0',null,null,null,'<p>新疆吐鲁番，独具特色的自然景观、丰富多彩的历史文化遗产，让这座城市散发着迷人的魅力。</p><p><br></p><p>8月，全国人大常委会执法检查组在新疆开展非物质文化遗产法执法检查。来到吐鲁番，坎儿井流水汩汩，诉说着各族群众的智慧。坎儿井是水利工程，是文物，还孕育了独特的非遗技艺——2014年，坎儿井开凿技艺被列入国家级非物质文化遗产代表性项目名录。</p><p>如何理解非物质文化遗产中的“非物质”？一位检查组成员的回答发人深省：非遗是一个整体性的文化概念，代表了一种文化生态。非遗中的“非物质”并不是说与物质无关，多数非遗以物质为依托，通过物质的媒介或载体反映其精神、价值、意义。</p><p>随后的行程中，检查组对《玛纳斯》、维吾尔族桑皮纸制作技艺、柯尔克孜族刺绣、俄罗斯族巴扬艺术等非遗项目的保护情况进行详细询问，与代表性传承人深入交流，听取非物质文化遗产法实施的意见建议。</p><p>碰撞交流中，一个观点逐渐清晰：非遗保护工作中，有必要进一步加强非遗的物质认同，透过“物质”看“非遗”，而不可偏废一端。</p><p>透过“物质”看“非遗”，就要保护好与非遗传承发展相关的工具、实物等。非物质文化遗产法于2011年公布施行，其明确规定，非物质文化遗产，是指各族人民世代相传并视为其文化遗产组成部分的各种传统文化表现形式，以及与传统文化表现形式相关的实物和场所。非遗的保护传承，建立在一定物质形态基础之上。许多民间艺术、生产技艺、民俗活动等非遗项目，都是依附于一定物质而存在的，实际保护工作实践中两者又是交织在一起的。正是对坎儿井的保护与修复，才有了开凿技艺的非遗传承。正如一位村民所说，坎儿井修到哪里，哪里就是绿洲，哪里就有文化传承。</p><p>透过“物质”看“非遗”，就要重视非遗代表性传承人的培养。非遗保护传承，关键在人。检查中了解到，不少非遗技艺、工序没有文字记录，有赖于师徒之间的口传心授、长期实践的耳濡目染。保持和扩大传承人群，加强非遗人才队伍建设，不断提升传承人素养，才能让非遗在传承中延续历史文脉，在当代生活中愈加枝繁叶茂。古琴艺术是中国走向世界的一张“非遗名片”，其传承一度濒危。上世纪60年代，全世界会弹古琴的人还不到200个。自2003年列入世界非遗名录以来，古琴艺术得到大力推广，代表性传承人通过演出、讲座等形式培育“粉丝群”，目前弹古琴学古琴的人数已达百万级。</p><p>透过“物质”看“非遗”，还要把非遗的保护传承和开发利用有机结合起来。在喀什香妃园，检查组看到，一些演艺、体验的旅游项目中融入了馕制作技艺、维吾尔族模制法土陶烧制技艺等非遗元素，吸引了许多游客互动参与。现在，越来越多的地方注重加强非遗与其他产业的融合发展，通过“非遗+文创”“非遗+旅游”“非遗+教育”“非遗+直播”等模式，拓宽非遗传承的渠道，让广大人民群众成为非遗保护的重要力量。</p><p>自非物质文化遗产法施行以来，比较健全的保护体系已逐步形成，相关部门法定责任落实、配套法规制度体系建设等都有了保障。一些地方将非遗连同其附着的物质空间、文化场合和话语氛围更好地协同保护起来，着手建立文化生态保护区。如今，修改非物质文化遗产法已列入十四届全国人大常委会立法规划，期待通过各方共同努力，进一步推动非遗法律制度体系健全完善，更好地守护好中华优秀传统文化的璀璨瑰宝。</p>','/api/upload/1844186525009444865.jpg',null);
insert into `article` values ('5','传承非遗技艺 共享文化魅力','非遗','925','0','2024-12-02 14:49:18.0','2024-12-02 14:49:18.0',null,null,null,'<p>非物质文化遗产是传统文化的精华、民族智慧的结晶。菏泽鲁西新区加大非遗项目、非遗工坊保护力度，通过“保护+传承”使非遗文化得到创新性发展，让非遗文化之美融入百姓生活。</p><p><br></p><p>在菏泽鲁西新区陈集镇曹州堆绣第七代传承人刘宪堂的展览馆里，一幅幅由花、鸟、鱼、虫等元素“堆”出的作品依次陈列在展台上。作为省级非物质文化遗产“曹州堆绣”第七代传承人，66岁的刘宪堂如今已经从事堆绣工作二十多年了，</p><p>菏泽曹州堆绣非遗传承人刘宪堂：“一幅作品比如说正常尺寸的60公分×60公分的这一种，大约就得5个工作日的时间，要是再大的，1米×2米的就得45天左右，我做那种四副屏得3个月。”</p><p>菏泽曹州堆绣起源于唐宋，盛行于明清。曹州堆绣被省文化厅授予“山东省农村特色文化品牌”，入选“省级非物质文化遗产名录”。刘宪堂延续先辈的技艺，吸取堆绣技艺的精髓，把平面堆绣和立体堆绣有机地融为一体，创作了一幅又一幅堆绣精品，先后荣获国内各级大奖40余项，本人也被4所高等院校聘为客座教授。</p><p>菏泽曹州堆绣非遗传承人刘宪堂：“现在我们的发展要根据我们的时代、社会的需求，去发展这个曹州堆绣，积极做一些非遗进校园活动，让一些学生年轻一代的重点了解曹州堆绣的工艺，把曹州堆绣的工艺更好地传承下去。”</p><p>为加强非遗的传承保护，菏泽鲁西新区成立了非遗传承实践基地，现有非物质文化遗产曹州面塑、根雕、堆绣、蛋雕、皮毛玩具等20类、500多件手工艺品，这些非物质文化遗产是鲁西新区文化的精髓，也是推进文化产业发展的宝贵资源。</p><p>菏泽鲁西新区文化和旅游办公室主任靳海峰：“鲁西新区坚持‘保护为主、抢救第一、合理利用、传承发展’原则，积极开展非物质文化遗产保护和传承工作，不断扩大项目保护范围，现有国家、省、市级非遗项目15个，市、区级非遗工坊19个。下一步，我们将充分挖掘非遗资源潜力，为具备传承条件的非遗项目及传承人提供平台。”</p>','/api/upload/1844185951765528577.jpg',null);
insert into `article` values ('6','品非遗美食 赏非遗文化','非遗','377','0','2024-12-02 14:49:18.0','2024-12-02 14:49:18.0',null,null,null,'<p>一对雌雄狮子，穿行于各种肤色的国际友人间，一段非遗舞狮表演迎来满场喝彩。10月8日晚，来自法国、意大利、马来西亚、哥伦比亚、克罗地亚的100余名艺术家，相聚山西老字号非遗美食馆林香斋，品鉴山西非遗美食，共赏国际非遗文化，上演一场中外非遗文化交流盛宴。</p><p>这是太原市非物质文化遗产协会与世界文化协会合作承办的非遗交流展。刚刚在清徐参加了第二届中法（太原·加纳）国际艺术周的国外艺术团成员们还沉浸在前几天激情欢快的情绪中，步入山西非遗美食集结地林香斋，听说这是一个有着近110年悠久历史的老字号品牌，发出啧啧惊叹。国外艺术家们次第出场，马来西亚的舞者舞动着古韵新姿的文化烂漫；斯里兰卡的艺术家演绎出热带国度特有的民族色彩；法国悠扬浪漫的华尔兹舞曲令人心旌荡漾；意大利舞蹈枝枝蔓蔓缠绕出旖旎的梦幻；还有来自哥伦比亚、圭亚那、克罗地亚的艺术家们纷纷献上精彩的表演。不同地域、不同文化、不同风格的舞蹈在这里交汇融合、激情碰撞，引起现场观众的热烈掌声。</p><p>台上，喜娘文化、吹面气球等山西非遗表演轮番上场。台下，山西过油肉、太原老火锅、太原醋鱼等非遗美食纷纷上桌。“我从未想象到面食会有这么神奇的力量，我会永远铭记这次难忘的旅程。”活动现场，非遗面食表演令来自哥伦比亚的艺术家非常激动。</p><p>太原市非物质文化遗产协会会长佟振宇介绍说，太原是一个文化底蕴深厚的城市，拥有国家级非物质文化遗产18项、省级非物质文化遗产96项、市级非物质文化遗产222项。这些非物质文化遗产犹如一颗颗璀璨的明珠，在岁月的长河中闪耀着独特的光芒。百年老店林香斋是山西的非遗美食基地，承载着山西面食、过油肉、太原醋鱼、太原铜火锅等山西非遗美食的传承使命，希望通过与世界各国非遗项目的交流，让太原乃至山西的非遗项目和非遗技艺走向世界。记者 李晓并 太原晚报</p>','/api/upload/1844186111220383745.jpg',null);
insert into `article_type` values ('1','100','非遗','0',null,null,null,'2024-12-02 14:49:18.0','2024-12-02 14:49:18.0');

UPDATE goods 
SET img = CONCAT('/api',img),
img_1 = CONCAT('/api',img_1),
img_2 = CONCAT('/api',img_2),
img_3 = CONCAT('/api',img_3),
img_4 = CONCAT('/api',img_4),
img_5 = CONCAT('/api',img_5);

UPDATE `cultural_and_creative_mall` 
SET cart_img = CONCAT('/api',cart_img),
cart_img_1 = CONCAT('/api',cart_img_1),
cart_img_2 = CONCAT('/api',cart_img_2),
cart_img_3 = CONCAT('/api',cart_img_3),
cart_img_4 = CONCAT('/api',cart_img_4),
cart_img_5 = CONCAT('/api',cart_img_5);
