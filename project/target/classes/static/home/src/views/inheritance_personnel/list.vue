<template>
	<div class="diy_list page_inheritance_personnel" id="inheritance_personnel_list">
		<div class="warp">
			<div class="container diy_list_container">
				<div class="diy_list_title">
					<div class="col">
						<span class="title">传承人员列表</span>
					</div>
				</div>
				<div class="leis_box"> 
				<div class="iudis_box">
			
				<div class="row diy_list_search">
					<div class="col">
						<!-- 搜索栏 -->
						<div class="view Search">
							<span class="diy_list_search_title">关键字搜索：</span>
																																												<b-form-input size="sm" class="mr-sm-2" placeholder="项目类别搜索" v-model="query['project_category']" />
																																																											<b-form-input size="sm" class="mr-sm-2" placeholder="人员姓名搜索" v-model="query['personnel_name']" />
																																																																																																																<b-button size="sm" @click="search()" >
								<b-icon icon="search"/>
							</b-button>
						</div>
						<!-- /搜索栏 -->
					</div>
				</div>
				<div class="diy_list_select_box">
					<span class="diy_list_select_title">下拉搜索：</span>
						<div class="diy_list_dropdown_box">
						<div class="col">
							<!-- 筛选 -->
							<div class="view sift">
												<b-dropdown text="项目类别" variant="outline-dark" left>
									<b-dropdown-item @click="filter_set('全部','project_category')">全部</b-dropdown-item>
										<b-dropdown-item v-for="(o, i) in list_project_category" :key="i" @click="filter_set(o['project_category'],'project_category')" >
												{{ o['project_category'] }}
										</b-dropdown-item>
								</b-dropdown>
																						<!-- 排序 -->
							
								<b-dropdown text="排序" variant="outline-dark" left>
										<b-dropdown-item v-for="(o, i) in list_sort" :key="i" @click="set_sort(o)" >
												{{ o.name }}
										</b-dropdown-item>
								</b-dropdown>
						
							<!--/排序 -->
							</div>
							<!-- /筛选 -->
						</div>
					</div>
				
				</div>
							</div>
				<div class="row diy_list_box">
					<div class="col">
						<!-- 列表 -->
						<list_inheritance_personnel :list="list" />
						<!-- /列表 -->
					</div>
				</div>
	</div>
	<!-- 大盒子结尾 -->
				<div class="row diy_list_page_box">
					<div class="col overflow-auto flex_cc">
						<!-- 分页器 -->
<!--						<diy_pager v-model="query['page']" :size="query['size']" :count="count" v-on:toPage="toPage" v-on:toSize="toSize" ></diy_pager>-->
            <b-pagination
                v-model="query.page"
                :total-rows="count"
                :per-page="query.size"
                @change="goToPage"
            />
						<!-- /分页器 -->
					</div>
				</div>
			</div>
		</div>
	</div>
</template>

<script>
	import list_inheritance_personnel from "@/components/diy/list_inheritance_personnel.vue";
	import diy_pager from "@/components/diy/diy_pager";
	import mixin from "@/mixins/page.js";

	export default {
		mixins: [mixin],
		components: {
			diy_pager,
			list_inheritance_personnel,
		},
		data() {
			return {
				url_get_list: "~/api/inheritance_personnel/get_list?like=0",

				// 查询条件
				query: {
					keyword: "",
					page: 1,
					size: 12,
									"project_category": "", // 项目类别
												"personnel_name": "", // 人员姓名
													},
				// 排序内容
				list_sort: [{
						name: "创建时间从高到低",
						value: "create_time desc",
					},
					{
						name: "创建时间从低到高",
						value: "create_time asc",
					},
					{
						name: "更新时间从高到低",
						value: "update_time desc",
					},
					{
						name: "更新时间从低到高",
						value: "update_time asc",
					},
														],

							// 项目类别列表
				"list_project_category": [""],
																		
			}
		},
		methods: {
      get_list_before(param) {
      },
			/**
			 * 筛选选择
			 */
			filter_set(o,key) {
			    if (o == "全部") {
			        this.query[key] = "";
			    } else {
			        this.query[key] = o;
			    }
			    this.search();
			},

			/**
			 * 排序
			 */
			set_sort(o) {
			    this.query.orderby = o.value;
			    this.search();
			},
					/**
			 * 获取项目类别列表
			 */
			async get_list_project_category() {
				var json = await this.$get("~/api/project_classification/get_list?");
				if (json.result) {
					this.list_project_category = json.result.list;
				} else if (json.error) {
					console.log(json.error);
				}
			},
												/**
			 * 筛选
			 */
							filter_project_category(o) {
				if (o == "全部") {
					this.query["project_category"] = "";
				} else {
					this.query["project_category"] = o;
				}
				this.search();
			},
																					/**
			 * 重置
			 */
			reset() {
								this.query.project_category = ""
											this.query.personnel_name = ""
													this.search();
			},

			// 返回条数
			toSize(i){
				this.query.size = i;
				this.first();
			},

			// 返回页数
			toPage(i){
				this.query.page = i;
				this.first();
			},

      goToPage(v){
        this.query.page = v;
        this.goToNew(v)
      },

		},
		computed: {
		},
		created() {
						/**
			 * 获取项目类别列表
			 */
			this.get_list_project_category();
																				}
	}
</script>

<style>
.el-date-editor--datetimerange.el-input, .el-date-editor--datetimerange.el-input__inner,.el-date-editor--daterange.el-input, .el-date-editor--daterange.el-input__inner, .el-date-editor--timerange.el-input, .el-date-editor--timerange.el-input__inner{
	width: 100%;
}
.el-date-editor .el-range-separator{
	width: 11%;
}
</style>
