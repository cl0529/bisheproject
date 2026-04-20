<template>
	<el-main class="bg table_wrap comtabel_t">
		<el-form label-position="right" :model="query" class="form p_4" label-width="120">
			<el-row class="rows row1">



							<el-col :xs="24" :sm="24" :lg="8" class="el_form_search_wrap">
					<el-form-item label="项目名称">
									<el-input v-model="query.entry_name"></el-input>
								</el-form-item>
				</el-col>
									<el-col :xs="24" :sm="24" :lg="8" class="el_form_search_wrap">
					<el-form-item label="地区名称">
									<el-select v-model="query.regional_name">
											<el-option v-for="o in list_regional_name" :key="o.regional_name" :label="o.regional_name"
								:value="o.regional_name">
							</el-option>
										</el-select>
								</el-form-item>
				</el-col>
									<el-col :xs="24" :sm="24" :lg="8" class="el_form_search_wrap">
					<el-form-item label="项目类别">
									<el-select v-model="query.project_category">
											<el-option v-for="o in list_project_category" :key="o.project_category" :label="o.project_category"
								:value="o.project_category">
							</el-option>
										</el-select>
								</el-form-item>
				</el-col>
																									</el-row>
	<el-row class="rows row2">

		<el-col :xs="24" :sm="24" :lg="24" class="search_btn_wrap search_btns">

				<el-col :xs="24" :sm="10" :lg="8" class="search_btn_1 search_btn_wrap_1 btns">

										<el-button type="primary" @click="search()" class="search_btn_find">查询</el-button>
						<el-button @click="reset()" style="margin-right: 74px;" class="search_btn_reset">重置</el-button>
																		

						<el-button v-if="$check_action('/intangible_cultural_heritage_projects/table','del') || $check_action('/intangible_cultural_heritage_projects/view','del')" class="search_btn_del" type="danger" @click="delInfo()">删除</el-button>
								
				</el-col>
		</el-col>
	</el-row >

		</el-form>
				<el-table :data="list" @selection-change="selectionChange" @sort-change="$sortChange" style="width: 100%" id="dataTable">
					<el-table-column fixed type="selection" tooltip-effect="dark" width="55">
			</el-table-column>
				<el-table-column prop="entry_name" @sort-change="$sortChange" label="项目名称" 				v-if="$check_field('get','entry_name')" min-width="200">
					</el-table-column>
					<el-table-column prop="regional_name" @sort-change="$sortChange" label="地区名称" 				v-if="$check_field('get','regional_name')" min-width="200">
					</el-table-column>
					<el-table-column prop="project_category" @sort-change="$sortChange" label="项目类别" 				v-if="$check_field('get','project_category')" min-width="200">
					</el-table-column>
					<el-table-column prop="cover_photo" @sort-change="$sortChange" label="封面图片" 				v-if="$check_field('get','cover_photo')" min-width="200">
						<template slot-scope="scope">
					<el-image style="width: 100px; height: 100px" :src="$fullUrl(scope.row['cover_photo'])"
						:preview-src-list="[$fullUrl(scope.row['cover_photo'])]">
						<div slot="error" class="image-slot">
							<img src="../../../public/img/error.png" style="width: 90px; height: 90px" />
						</div>
					</el-image>
				</template>
					</el-table-column>
					<el-table-column prop="inheritance_personnel" @sort-change="$sortChange" label="传承人员" 				v-if="$check_field('get','inheritance_personnel')" min-width="200">
						<template slot-scope="scope">
					{{ get_user_inheritance_personnel(scope.row['inheritance_personnel']) }}
				</template>
					</el-table-column>
					<el-table-column prop="personnel_name" @sort-change="$sortChange" label="人员姓名" 				v-if="$check_field('get','personnel_name')" min-width="200">
					</el-table-column>
					<el-table-column prop="announcement_date" @sort-change="$sortChange" label="公布日期" 				v-if="$check_field('get','announcement_date')" min-width="200">
		                <template slot-scope="scope">
                	{{ $toTime(scope.row["announcement_date"],"yyyy-MM-dd") }}
                </template>
					</el-table-column>
					<el-table-column prop="declaration_unit" @sort-change="$sortChange" label="申报单位" 				v-if="$check_field('get','declaration_unit')" min-width="200">
					</el-table-column>
					<el-table-column prop="protecting_units" @sort-change="$sortChange" label="保护单位" 				v-if="$check_field('get','protecting_units')" min-width="200">
					</el-table-column>
					<el-table-column prop="detailed_introduction" @sort-change="$sortChange" label="详细介绍" 				v-if="$check_field('get','detailed_introduction')" min-width="200">
					</el-table-column>
	


            <el-table-column sortable prop="create_time" label="创建时间" min-width="200">
                <template slot-scope="scope">
                	{{ $toTime(scope.row["create_time"],"yyyy-MM-dd hh:mm:ss") }}
                </template>
            </el-table-column>

			<el-table-column sortable prop="update_time" label="更新时间" min-width="200">
                <template slot-scope="scope">
                	{{ $toTime(scope.row["update_time"],"yyyy-MM-dd hh:mm:ss") }}
                </template>
			</el-table-column>







			<el-table-column fixed="right" label="操作" min-width="200" v-if="$check_action('/intangible_cultural_heritage_projects/table','set') || $check_action('/intangible_cultural_heritage_projects/view','set') || $check_action('/intangible_cultural_heritage_projects/view','get') 
						" >


				<template slot-scope="scope">
					<div class="view_a">
					<router-link class="e-button el-button--small is-plain el-button--success" style="margin: 5px !important;"
					v-if="$check_action('/intangible_cultural_heritage_projects/table','set') || $check_action('/intangible_cultural_heritage_projects/view','set') || $check_action('/intangible_cultural_heritage_projects/view','get')"
						:to="'./view?' + field + '=' + scope.row[field]"
						 size="small">
						<span>详情</span>
					</router-link>
						<router-link v-if="$check_comment('/intangible_cultural_heritage_projects/table')" class="e-button el-button--small is-plain el-button--primary"
								  :to="'../comment/table?size=10&page=1&source_table=intangible_cultural_heritage_projects&source_field=' + field + '&source_id=' + scope.row[field]" size="small">
						<span>查看评论</span>
					</router-link>
					</div>
				</template>
			</el-table-column>

		</el-table>

		<!-- 分页器 -->
		<div class="mt text_center">
			<el-pagination @size-change="handleSizeChange" @current-change="handleCurrentChange"
				:current-page="query.page" :page-sizes="[7, 10, 30, 100]" :page-size="query.size"
				layout="total, sizes, prev, pager, next, jumper" :total="count">
			</el-pagination>
		</div>
		<!-- /分页器 -->
										
		<div class="modal_wrap" v-if="showModal">
			<div class="modal_box">
				<!-- <div class="modal_box_close" @click="closeModal">X</div> -->
				<p class="modal_box_title">重要提醒</p>
				<p class="modal_box_text">当前有数据达到预警值！</p>
				<p class="modal_box_text">{{ message }}</p>
				<div class="btn_box">
					<span @click="closeModal">取消</span>
					<span @click="closeModal">确定</span>
				</div>
			</div>
		</div>


	</el-main>
</template>
<script>
	import mixin from "@/mixins/page.js";

	export default {
		mixins: [mixin],
		data() {
			return {
				// 弹框
				showModal: false,
				// 获取数据地址
				url_get_list: "~/api/intangible_cultural_heritage_projects/get_list?like=0",
				url_del: "~/api/intangible_cultural_heritage_projects/del?",

				// 字段ID
				field: "intangible_cultural_heritage_projects_id",

				// 查询
				query: {
					"size": 7,
					"page": 1,
								"entry_name": "",
											"regional_name": "",
											"project_category": "",
															"login_time": "",
					"create_time": "",
					"orderby": `create_time desc`
				},

				// 数据
				list: [],
																	// 地区名称列表
				list_regional_name: [""],
							// 项目类别列表
				list_project_category: [""],
												// 用户列表
				list_user_inheritance_personnel: [],
															message: '',
			}
		},
		methods: {
			// 关闭弹框
			closeModal(){
				this.showModal = false;
				},
			/**
			 * @description 获取到列表事件
			 * @param {Object} res 响应结果
			 */
			get_list_after: function get_list_after(res, func, url) {
				let _this = this
																																																									},
			get_list_before(param){
				var user_group = this.user.user_group;
				if(user_group != "管理员"){
						let sqlwhere = "(";
																														if(user_group=="传承用户"){
						sqlwhere+= "inheritance_personnel = " + this.user.user_id + " or ";
					}
																																			if (sqlwhere.length>1){
						sqlwhere = sqlwhere.substr(0,sqlwhere.length-4);
						sqlwhere += ")";
						param["sqlwhere"] = sqlwhere;
					}
					}
				return param;
			},

						/**
			 * 获取地区名称列表
			 */
			async get_list_regional_name() {
				var json = await this.$get("~/api/regional_classification/get_list?");
				if(json.result){
					this.list_regional_name = json.result.list;
				}else if (json.error){
					console.log(json.error);
				}
			},
						/**
			 * 获取项目类别列表
			 */
			async get_list_project_category() {
				var json = await this.$get("~/api/project_classification/get_list?");
				if(json.result){
					this.list_project_category = json.result.list;
				}else if (json.error){
					console.log(json.error);
				}
			},
																



												/**
			 * 获取传承用户用户列表
			 */
			async get_list_user_inheritance_personnel() {
				var json = await this.$get("~/api/user/get_list?user_group=传承用户");
				if(json.result && json.result.list){
					this.list_user_inheritance_personnel = json.result.list;
				}
				else if(json.error){
					console.error(json.error);
				}
			},

			get_user_inheritance_personnel(id){
				var obj = this.list_user_inheritance_personnel.getObj({"user_id":id});
				var ret = "";
				if(obj){
					ret = obj.nickname+"-"+obj.username;
					// if(obj.nickname){
					// 	ret = obj.nickname;
					// }
					// else{
					// 	ret = obj.username;
					// }
				}
				return ret;
			},
														},
				created() {
						// 初始化地区名称列表
			this.get_list_regional_name();
						// 初始化项目类别列表
			this.get_list_project_category();
											this.get_list_user_inheritance_personnel();
													}
	}
</script>

<style type="text/css">
	.bg {
		background: white;
	}

	.form.p_4 {
		padding: 1rem;
	}

	.form .el-input {
		width: initial;
	}

	.mt {
		margin-top: 1rem;
	}

	.text_center {
		text-align: center;
	}

	.float-right {
		float: right;
	}


	.modal_wrap{
		width: 100vw;
		height: 100vh;
		position: fixed;
		top: 0;
		left: 0;
		background: rgba(0,0,0,0.5);
		z-index: 9999999999;
	}
	.modal_wrap .modal_box{
		width: 400px;
		height: auto;
		background: url("../../assets/modal_bg.jpg") no-repeat center;
		background-size: cover;
		position: absolute;
		top: 50%;
		left: 50%;
		margin-left: -200px;
		margin-top: -100px;
		border-radius: 10px;
		padding: 10px;
		}
	.modal_wrap .modal_box .modal_box_close{
		font-size: 20px;
		position: absolute;
		top: 10px;
		right: 10px;
		cursor: pointer;
		}
	.modal_wrap .modal_box .modal_box_title{
	  text-align: center;
    font-size: 18px;
    margin: 16px auto;
    color: #fff;
    border-bottom: 1px solid rgba(117, 116, 116,0.5);
    padding-bottom: 16px;
    width: 356px;
		}
	.modal_wrap .modal_box .modal_box_text{
		text-align: center;
		font-size: 14px;
		color: #fff;
		margin: 5px auto;
		width: 90%;
		}
	.modal_wrap .modal_box .btn_box{
		display: flex;
		flex-direction: row;
		justify-content: center;
		margin-top: 42px;
		margin-bottom: 20px;
		}
			.modal_wrap .modal_box .btn_box span{
				display: inline-block;
				width: 80px;
				height: 30px;
				line-height: 30px;
				text-align: center;
				border: 1px solid #ccc;
				font-size: 14px;
				cursor: pointer;
				color: #fff;
			}
	.modal_wrap .modal_box .btn_box span:nth-child(2){
		background: #409EFF;
		color: #fff;
		border-color: #409EFF;
		margin-left: 15px;
	}
</style>
