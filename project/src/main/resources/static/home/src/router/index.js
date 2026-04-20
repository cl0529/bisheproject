import Vue from 'vue';
import VueRouter from 'vue-router';
import index from '../views/index.vue'
import login from '../views/account/login.vue';
Vue.use(VueRouter)

const routes = [
	// 主页
	{
		path: '/',
		name: 'index',
		component: index
	},
	// 登录
	{
		path: '/account/login',
		name: 'login',
		component: login
	},
	// 忘记密码
	{
		path: '/account/forgot',
		name: 'forgot',
		component: () => import('../views/account/forgot.vue')
	},
	// 注册账号
	{
		path: '/account/register',
		name: 'register',
		component: () => import('../views/account/register.vue')
	},
		// 媒体图片
	{
		path: '/media/image',
		name: 'media_image',
		component: () => import('../views/media/image.vue')
	},
	// 音乐
	{
		path: '/media/music',
		name: 'media_music',
		component: () => import('../views/media/music.vue')
	},
	// 媒体视频
	{
		path: '/media/video',
		name: 'media_video',
		component: () => import('../views/media/video.vue')
	},
	// 媒体视频
	{
		path: '/user_center/index',
		name: 'user_center_index',
		component: () => import('../views/user_center/index.vue')
	},
	// 文章路由
	{
		path: '/article/list',
		name: 'article_list',
		component: () => import('../views/article/list.vue')
	},
	{
		path: '/article/details',
		name: 'article_details',
		component: () => import('../views/article/details.vue')
	},
	// 浏览网站
	// 收藏路由
	{
		path: '/user/collect',
		name: 'collect_list',
		component: () => import('../views/user/collect.vue')
	},


	{
		path: '/comment/table',
		name: 'comment_table',
		component: () => import('../views/comment/table.vue')
	},
	{
		path: '/comment/view',
		name: 'comment_view',
		component: () => import('../views/comment/view.vue')
	},

	

	// 商品路由
	{
		path: '/goods/list',
		name: 'goods_list',
		component: () => import('../views/goods/list.vue')
	},
	{
		path: '/goods/details',
		name: 'goods_details',
		component: () => import('../views/goods/details.vue')
	},
		{
		path: '/pay/detail',
		name: 'pay_detail',
		component: () => import('../views/pay/details.vue')
	},
	// 购物车路由
	{
		path: '/cart/list',
		name: 'cart_list',
		component: () => import('../views/cart/list.vue')
	},
	// 订单路由
	{
		path: '/order/list',
		name: 'order_list',
		component: () => import('../views/order/list.vue')
	},
	{
		path: '/order/details',
		name: 'order_details',
		component: () => import('../views/order/details.vue')
	},
		// 物流配送表格路由
	{
		path: '/logistics_delivery/table',
		name: '/logistics_delivery_table',
		component: () => import('../views/logistics_delivery/table.vue')
	},
	// 物流配送详情路由
	{
		path: '/logistics_delivery/view',
		name: '/logistics_delivery_view',
		component: () => import('../views/logistics_delivery/view.vue')
	},
	// 收货地址路由
	{
		path: '/user/address',
		name: 'address',
		component: () => import('../views/user/address.vue')
	},
	{
		path: '/user/address_edit',
		name: 'address_edit',
		component: () => import('../views/user/address_edit.vue')
	},
	

	// 公告路由
	{
		path: '/notice/list',
		name: 'notice_list',
		component: () => import('../views/notice/list.vue')
	},
	{
		path: '/notice/details',
		name: 'notice_details',
		component: () => import('../views/notice/details.vue')
	},
	// 普通用户表格路由
	{
		path: '/ordinary_users/table',
		name: '/ordinary_users_table',
		component: () => import('../views/ordinary_users/table.vue')
	},
	// 普通用户详情路由
	{
		path: '/ordinary_users/view',
		name: '/ordinary_users_view',
		component: () => import('../views/ordinary_users/view.vue')
	},
	
	
		
		// 传承用户表格路由
	{
		path: '/inheriting_users/table',
		name: '/inheriting_users_table',
		component: () => import('../views/inheriting_users/table.vue')
	},
	// 传承用户详情路由
	{
		path: '/inheriting_users/view',
		name: '/inheriting_users_view',
		component: () => import('../views/inheriting_users/view.vue')
	},
	
	
		
		// 文创商城表格路由
	{
		path: '/cultural_and_creative_mall/table',
		name: '/cultural_and_creative_mall_table',
		component: () => import('../views/cultural_and_creative_mall/table.vue')
	},
	// 文创商城详情路由
	{
		path: '/cultural_and_creative_mall/view',
		name: '/cultural_and_creative_mall_view',
		component: () => import('../views/cultural_and_creative_mall/view.vue')
	},
	
		// 文创商城列表路由
	{
		path: '/cultural_and_creative_mall/list',
		name: '/cultural_and_creative_mall_list',
		component: () => import('../views/cultural_and_creative_mall/list.vue')
	},
	
		// 文创商城详情路由
	{
		path: '/cultural_and_creative_mall/details',
		name: '/cultural_and_creative_mall_details',
		component: () => import('../views/cultural_and_creative_mall/details.vue')
	},
		
		// 项目分类表格路由
	{
		path: '/project_classification/table',
		name: '/project_classification_table',
		component: () => import('../views/project_classification/table.vue')
	},
	// 项目分类详情路由
	{
		path: '/project_classification/view',
		name: '/project_classification_view',
		component: () => import('../views/project_classification/view.vue')
	},
	
	
		
		// 地区分类表格路由
	{
		path: '/regional_classification/table',
		name: '/regional_classification_table',
		component: () => import('../views/regional_classification/table.vue')
	},
	// 地区分类详情路由
	{
		path: '/regional_classification/view',
		name: '/regional_classification_view',
		component: () => import('../views/regional_classification/view.vue')
	},
	
	
		
		// 非遗项目表格路由
	{
		path: '/intangible_cultural_heritage_projects/table',
		name: '/intangible_cultural_heritage_projects_table',
		component: () => import('../views/intangible_cultural_heritage_projects/table.vue')
	},
	// 非遗项目详情路由
	{
		path: '/intangible_cultural_heritage_projects/view',
		name: '/intangible_cultural_heritage_projects_view',
		component: () => import('../views/intangible_cultural_heritage_projects/view.vue')
	},
	
		// 非遗项目列表路由
	{
		path: '/intangible_cultural_heritage_projects/list',
		name: '/intangible_cultural_heritage_projects_list',
		component: () => import('../views/intangible_cultural_heritage_projects/list.vue')
	},
	
		// 非遗项目详情路由
	{
		path: '/intangible_cultural_heritage_projects/details',
		name: '/intangible_cultural_heritage_projects_details',
		component: () => import('../views/intangible_cultural_heritage_projects/details.vue')
	},
		
		// 传承人员表格路由
	{
		path: '/inheritance_personnel/table',
		name: '/inheritance_personnel_table',
		component: () => import('../views/inheritance_personnel/table.vue')
	},
	// 传承人员详情路由
	{
		path: '/inheritance_personnel/view',
		name: '/inheritance_personnel_view',
		component: () => import('../views/inheritance_personnel/view.vue')
	},
	
		// 传承人员列表路由
	{
		path: '/inheritance_personnel/list',
		name: '/inheritance_personnel_list',
		component: () => import('../views/inheritance_personnel/list.vue')
	},
	
		// 传承人员详情路由
	{
		path: '/inheritance_personnel/details',
		name: '/inheritance_personnel_details',
		component: () => import('../views/inheritance_personnel/details.vue')
	},
		
	
	// 用户路由
	{
		path: '/user/index',
		name: 'user_index',
		component: () => import('../views/user/index.vue')
	},
	// 基本信息
	{
		path: '/user/info',
		name: 'user_info',
		component: () => import('../views/user/info.vue')
	},
	// 找回密码
	{
		path: '/user/password',
		name: 'user_password',
		component: () => import('../views/user/password.vue')
	},

	// 搜索
	{
		path: '/search',
		name: 'search',
		component: () => import('../views/search/index.vue')
	},
	// 局部搜索
	{
		path: '/search/details',
		name: 'search_details',
		component: () => import('../views/search/details.vue')
	}
]

const router = new VueRouter({
	mode: 'hash',
	base: process.env.BASE_URL,
	routes
})

router.afterEach((to, from, next) => {
	let title = "非遗助推平台-home";
	document.title = title;
	document.logo = "非遗助推平台"
})

export default router
