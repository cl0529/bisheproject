<template>
  <div class="page_search search_index">
	<div class="warp">
	  <div class="container">
		<div class="row">
		  <div class="col-12">
			<div class="card_result_search">
			  <div class="title">搜索结果</div>
				<!-- 商品搜索结果 -->
			  <list_result_search
				:list="result_goods"
				title="文创商城"
				source_table="goods"
			  ></list_result_search>

				<!-- 文章搜索结果 -->
			  <list_result_search
				:list="result_article"
				title="新闻资讯"
				source_table="article"
			  ></list_result_search>


						  <list_result_search
				v-if="$check_action('/ordinary_users/list', 'get')"
				:list="result_ordinary_users_user_name"
				title="普通用户用户姓名"
				source_table="ordinary_users"
			  ></list_result_search>
															  <list_result_search
				v-if="$check_action('/inheriting_users/list', 'get')"
				:list="result_inheriting_users_personnel_name"
				title="传承用户人员姓名"
				source_table="inheriting_users"
			  ></list_result_search>
																								  <list_result_search
				v-if="$check_action('/cultural_and_creative_mall/list', 'get')"
				:list="result_cultural_and_creative_mall_personnel_name"
				title="文创商城人员姓名"
				source_table="cultural_and_creative_mall"
			  ></list_result_search>
												  <list_result_search
				v-if="$check_action('/project_classification/list', 'get')"
				:list="result_project_classification_project_category"
				title="项目分类项目类别"
				source_table="project_classification"
			  ></list_result_search>
									  <list_result_search
				v-if="$check_action('/regional_classification/list', 'get')"
				:list="result_regional_classification_regional_name"
				title="地区分类地区名称"
				source_table="regional_classification"
			  ></list_result_search>
									  <list_result_search
				v-if="$check_action('/intangible_cultural_heritage_projects/list', 'get')"
				:list="result_intangible_cultural_heritage_projects_entry_name"
				title="非遗项目项目名称"
				source_table="intangible_cultural_heritage_projects"
			  ></list_result_search>
								  <list_result_search
				v-if="$check_action('/intangible_cultural_heritage_projects/list', 'get')"
				:list="result_intangible_cultural_heritage_projects_regional_name"
				title="非遗项目地区名称"
				source_table="intangible_cultural_heritage_projects"
			  ></list_result_search>
								  <list_result_search
				v-if="$check_action('/intangible_cultural_heritage_projects/list', 'get')"
				:list="result_intangible_cultural_heritage_projects_project_category"
				title="非遗项目项目类别"
				source_table="intangible_cultural_heritage_projects"
			  ></list_result_search>
																																	  <list_result_search
				v-if="$check_action('/inheritance_personnel/list', 'get')"
				:list="result_inheritance_personnel_project_category"
				title="传承人员项目类别"
				source_table="inheritance_personnel"
			  ></list_result_search>
											  <list_result_search
				v-if="$check_action('/inheritance_personnel/list', 'get')"
				:list="result_inheritance_personnel_personnel_name"
				title="传承人员人员姓名"
				source_table="inheritance_personnel"
			  ></list_result_search>
																								</div>
		  </div>
		</div>
	  </div>
	</div>
  </div>
</template>

<script>
import mixin from "../../mixins/page.js";
import list_result_search from "../../components/diy/list_result_search.vue";

export default {
  mixins: [mixin],
  data() {
	return {
	  "query": {
		word: "",
	  },
	  "result_goods": [],
	  "result_article": [],
						"result_ordinary_users_user_name":[],
															"result_inheriting_users_personnel_name":[],
																								"result_cultural_and_creative_mall_personnel_name":[],
												"result_project_classification_project_category":[],
									"result_regional_classification_regional_name":[],
									"result_intangible_cultural_heritage_projects_entry_name":[],
								"result_intangible_cultural_heritage_projects_regional_name":[],
								"result_intangible_cultural_heritage_projects_project_category":[],
																																	"result_inheritance_personnel_project_category":[],
											"result_inheritance_personnel_personnel_name":[],
																						};
  },
  methods: {
	/**
	 * 获取${fmodel.filter.cart_name}
	 */
	get_goods() {
	  this.$get("~/api/goods/get_list?like=0", { page: 1, size: 10, title: this.query.word }, (json) => {
		if (json.result) {
		  this.result_goods = json.result.list;
		}
	  });
	},
	/**
	 * 获取文章
	 */
	get_article() {
	  this.$get("~/api/article/get_list?like=0", { page: 1, size: 10, title: this.query.word }, (json) => {
		if (json.result) {
		  this.result_article = json.result.list;
		}
	  });
	},

				/**
	 * 获取user_name
	 */
	get_ordinary_users_user_name(){
		let url = "~/api/ordinary_users/get_list?like=0";
				this.$get(url, { page: 1, size: 10, "user_name": this.query.word }, (json) => {
		  if (json.result) {
			var result_ordinary_users_user_name = json.result.list;
			result_ordinary_users_user_name.map(o => o.title = o['user_name'])
	  			this.result_ordinary_users_user_name = result_ordinary_users_user_name
		 	}
		});
	},
													/**
	 * 获取personnel_name
	 */
	get_inheriting_users_personnel_name(){
		let url = "~/api/inheriting_users/get_list?like=0";
				url = url+"&examine_state=已通过";
				this.$get(url, { page: 1, size: 10, "personnel_name": this.query.word }, (json) => {
		  if (json.result) {
			var result_inheriting_users_personnel_name = json.result.list;
			result_inheriting_users_personnel_name.map(o => o.title = o['personnel_name'])
	  			this.result_inheriting_users_personnel_name = result_inheriting_users_personnel_name
		 	}
		});
	},
																						/**
	 * 获取personnel_name
	 */
	get_cultural_and_creative_mall_personnel_name(){
		let url = "~/api/cultural_and_creative_mall/get_list?like=0";
				this.$get(url, { page: 1, size: 10, "personnel_name": this.query.word }, (json) => {
		  if (json.result) {
			var result_cultural_and_creative_mall_personnel_name = json.result.list;
			result_cultural_and_creative_mall_personnel_name.map(o => o.title = o['personnel_name'])
	  			this.result_cultural_and_creative_mall_personnel_name = result_cultural_and_creative_mall_personnel_name
		 	}
		});
	},
										/**
	 * 获取project_category
	 */
	get_project_classification_project_category(){
		let url = "~/api/project_classification/get_list?like=0";
				this.$get(url, { page: 1, size: 10, "project_category": this.query.word }, (json) => {
		  if (json.result) {
			var result_project_classification_project_category = json.result.list;
			result_project_classification_project_category.map(o => o.title = o['project_category'])
	  			this.result_project_classification_project_category = result_project_classification_project_category
		 	}
		});
	},
							/**
	 * 获取regional_name
	 */
	get_regional_classification_regional_name(){
		let url = "~/api/regional_classification/get_list?like=0";
				this.$get(url, { page: 1, size: 10, "regional_name": this.query.word }, (json) => {
		  if (json.result) {
			var result_regional_classification_regional_name = json.result.list;
			result_regional_classification_regional_name.map(o => o.title = o['regional_name'])
	  			this.result_regional_classification_regional_name = result_regional_classification_regional_name
		 	}
		});
	},
							/**
	 * 获取entry_name
	 */
	get_intangible_cultural_heritage_projects_entry_name(){
		let url = "~/api/intangible_cultural_heritage_projects/get_list?like=0";
				this.$get(url, { page: 1, size: 10, "entry_name": this.query.word }, (json) => {
		  if (json.result) {
			var result_intangible_cultural_heritage_projects_entry_name = json.result.list;
			result_intangible_cultural_heritage_projects_entry_name.map(o => o.title = o['entry_name'])
	  			this.result_intangible_cultural_heritage_projects_entry_name = result_intangible_cultural_heritage_projects_entry_name
		 	}
		});
	},
						/**
	 * 获取regional_name
	 */
	get_intangible_cultural_heritage_projects_regional_name(){
		let url = "~/api/intangible_cultural_heritage_projects/get_list?like=0";
				this.$get(url, { page: 1, size: 10, "regional_name": this.query.word }, (json) => {
		  if (json.result) {
			var result_intangible_cultural_heritage_projects_regional_name = json.result.list;
			result_intangible_cultural_heritage_projects_regional_name.map(o => o.title = o['regional_name'])
	  			this.result_intangible_cultural_heritage_projects_regional_name = result_intangible_cultural_heritage_projects_regional_name
		 	}
		});
	},
						/**
	 * 获取project_category
	 */
	get_intangible_cultural_heritage_projects_project_category(){
		let url = "~/api/intangible_cultural_heritage_projects/get_list?like=0";
				this.$get(url, { page: 1, size: 10, "project_category": this.query.word }, (json) => {
		  if (json.result) {
			var result_intangible_cultural_heritage_projects_project_category = json.result.list;
			result_intangible_cultural_heritage_projects_project_category.map(o => o.title = o['project_category'])
	  			this.result_intangible_cultural_heritage_projects_project_category = result_intangible_cultural_heritage_projects_project_category
		 	}
		});
	},
																															/**
	 * 获取project_category
	 */
	get_inheritance_personnel_project_category(){
		let url = "~/api/inheritance_personnel/get_list?like=0";
				this.$get(url, { page: 1, size: 10, "project_category": this.query.word }, (json) => {
		  if (json.result) {
			var result_inheritance_personnel_project_category = json.result.list;
			result_inheritance_personnel_project_category.map(o => o.title = o['project_category'])
	  			this.result_inheritance_personnel_project_category = result_inheritance_personnel_project_category
		 	}
		});
	},
									/**
	 * 获取personnel_name
	 */
	get_inheritance_personnel_personnel_name(){
		let url = "~/api/inheritance_personnel/get_list?like=0";
				this.$get(url, { page: 1, size: 10, "personnel_name": this.query.word }, (json) => {
		  if (json.result) {
			var result_inheritance_personnel_personnel_name = json.result.list;
			result_inheritance_personnel_personnel_name.map(o => o.title = o['personnel_name'])
	  			this.result_inheritance_personnel_personnel_name = result_inheritance_personnel_personnel_name
		 	}
		});
	},
																					
  },
  components: { list_result_search },
	created(){
    this.query.word = this.$route.query.word || "";
  },
  mounted() {
	this.get_goods();
	this.get_article();
					this.get_ordinary_users_user_name();
														this.get_inheriting_users_personnel_name();
																							this.get_cultural_and_creative_mall_personnel_name();
											this.get_project_classification_project_category();
								this.get_regional_classification_regional_name();
								this.get_intangible_cultural_heritage_projects_entry_name();
							this.get_intangible_cultural_heritage_projects_regional_name();
							this.get_intangible_cultural_heritage_projects_project_category();
																																this.get_inheritance_personnel_project_category();
										this.get_inheritance_personnel_personnel_name();
																					  },
  watch: {
	$route() {
	  $.push(this.query, this.$route.query);
	  this.get_goods();
	  this.get_article();
				  this.get_ordinary_users_user_name();
													  this.get_inheriting_users_personnel_name();
																						  this.get_cultural_and_creative_mall_personnel_name();
										  this.get_project_classification_project_category();
							  this.get_regional_classification_regional_name();
							  this.get_intangible_cultural_heritage_projects_entry_name();
						  this.get_intangible_cultural_heritage_projects_regional_name();
						  this.get_intangible_cultural_heritage_projects_project_category();
																															  this.get_inheritance_personnel_project_category();
									  this.get_inheritance_personnel_personnel_name();
																						},
  },
};
</script>

<style scoped>
.card_search {
  text-align: center;
}
.card_result_search>.title {
  text-align: center;
  padding: 10px 0;
}
</style>
