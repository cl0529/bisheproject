<template>
	<el-main class="bg edit_wrap comtable_e">
		<el-form ref="form" :model="form" status-icon label-width="120px" v-if="is_view()">
		<el-row class="row_ce"> 
							<el-col v-if="$check_field('get','entry_name') || $check_field('add','entry_name') || $check_field('set','entry_name')" :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
							<el-form-item label="项目名称" prop="entry_name">
															<el-input id="entry_name" v-model="form['entry_name']" placeholder="请输入项目名称"
							  v-if="(form['intangible_cultural_heritage_projects_id'] && $check_field('set','entry_name')) || (!form['intangible_cultural_heritage_projects_id'] && $check_field('add','entry_name'))" :disabled="disabledObj['entry_name_isDisabled']"></el-input>
					<div v-else-if="$check_field('get','entry_name')">{{form['entry_name']}}</div>
											</el-form-item>
			</el-col>
								<el-col v-if="$check_field('get','regional_name') || $check_field('add','regional_name') || $check_field('set','regional_name')" :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
							<el-form-item label="地区名称" prop="regional_name">
											<el-select id="regional_name" v-model="form['regional_name']"						v-if="(form['intangible_cultural_heritage_projects_id'] && $check_field('set','regional_name')) || (!form['intangible_cultural_heritage_projects_id'] && $check_field('add','regional_name'))">
						<el-option v-for="o in list_regional_name" :key="o['regional_name']" :label="o['regional_name']"
							:value="o['regional_name']">
						</el-option>
					</el-select>
					<div v-else-if="$check_field('get','regional_name')">{{form['regional_name']}}</div>
							</el-form-item>
			</el-col>
								<el-col v-if="$check_field('get','project_category') || $check_field('add','project_category') || $check_field('set','project_category')" :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
							<el-form-item label="项目类别" prop="project_category">
											<el-select id="project_category" v-model="form['project_category']"						v-if="(form['intangible_cultural_heritage_projects_id'] && $check_field('set','project_category')) || (!form['intangible_cultural_heritage_projects_id'] && $check_field('add','project_category'))">
						<el-option v-for="o in list_project_category" :key="o['project_category']" :label="o['project_category']"
							:value="o['project_category']">
						</el-option>
					</el-select>
					<div v-else-if="$check_field('get','project_category')">{{form['project_category']}}</div>
							</el-form-item>
			</el-col>
								<el-col v-if="$check_field('get','cover_photo') || $check_field('add','cover_photo') || $check_field('set','cover_photo')" :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
							<el-form-item label="封面图片" prop="cover_photo">
											<el-upload :disabled="disabledObj['cover_photo_isDisabled']" class="avatar-uploader" drag
						accept="image/gif, image/jpeg, image/png, image/jpg" action="" :http-request="upload_cover_photo"
						:show-file-list="false" v-if="(form['intangible_cultural_heritage_projects_id'] && $check_field('set','cover_photo')) || (!form['intangible_cultural_heritage_projects_id'] && $check_field('add','cover_photo'))">
						<img id="cover_photo" v-if="form['cover_photo']" :src="$fullUrl(form['cover_photo'])" class="avatar">
						<i v-else class="el-icon-plus avatar-uploader-icon"></i>
					</el-upload>
					<el-image v-else-if="$check_field('get','cover_photo')" style="width: 100px; height: 100px"
						:src="$fullUrl(form['cover_photo'])" :preview-src-list="[$fullUrl(form['cover_photo'])]">
						<div slot="error" class="image-slot">
							<img src="../../../public/img/error.png" style="width: 90px; height: 90px" />
						</div>
					</el-image>
							</el-form-item>
			</el-col>
								<el-col v-if="$check_field('get','inheritance_personnel') || $check_field('add','inheritance_personnel') || $check_field('set','inheritance_personnel')" :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
							<el-form-item label="传承人员" prop="inheritance_personnel">
																					<div v-if="user_group !== '管理员'">
							{{ get_user_session_inheritance_personnel(form['inheritance_personnel']) }}
							<el-select v-if="(form['intangible_cultural_heritage_projects_id'] && $check_field('set','inheritance_personnel')) || (!form['intangible_cultural_heritage_projects_id'] && $check_field('add','inheritance_personnel'))" id="inheritance_personnel" v-model="form['inheritance_personnel']" :disabled="disabledObj['inheritance_personnel_isDisabled']">
								<el-option v-for="o in list_user_inheritance_personnel" :key="o['username']" :label="o['nickname'] + '-' + o['username']"
										   :value="o['user_id']">
								</el-option>
							</el-select>
							<el-select v-else-if="$check_field('get','inheritance_personnel')" id="inheritance_personnel" v-model="form['inheritance_personnel']" :disabled="true">
								<el-option v-for="o in list_user_inheritance_personnel" :key="o['username']" :label="o['nickname'] + '-' + o['username']"
										   :value="o['user_id']">
								</el-option>
							</el-select>
						</div>
						<el-select v-else id="inheritance_personnel" v-model="form['inheritance_personnel']" :disabled="disabledObj['inheritance_personnel_isDisabled']">
							<el-option v-for="o in list_user_inheritance_personnel" :key="o['username']" :label="o['nickname'] + '-' + o['username']"
									   :value="o['user_id']">
							</el-option>
						</el-select>
																</el-form-item>
			</el-col>
								<el-col v-if="$check_field('get','personnel_name') || $check_field('add','personnel_name') || $check_field('set','personnel_name')" :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
							<el-form-item label="人员姓名" prop="personnel_name">
															<el-input id="personnel_name" v-model="form['personnel_name']" placeholder="请输入人员姓名"
							  v-if="(form['intangible_cultural_heritage_projects_id'] && $check_field('set','personnel_name')) || (!form['intangible_cultural_heritage_projects_id'] && $check_field('add','personnel_name'))" :disabled="disabledObj['personnel_name_isDisabled']"></el-input>
					<div v-else-if="$check_field('get','personnel_name')">{{form['personnel_name']}}</div>
											</el-form-item>
			</el-col>
								<el-col v-if="$check_field('get','announcement_date') || $check_field('add','announcement_date') || $check_field('set','announcement_date')" :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
							<el-form-item label="公布日期" prop="announcement_date">
											<el-date-picker :disabled="disabledObj['announcement_date_isDisabled']" v-if="(form['intangible_cultural_heritage_projects_id'] && $check_field('set','announcement_date')) || (!form['intangible_cultural_heritage_projects_id'] && $check_field('add','announcement_date'))" id="announcement_date"
						v-model="form['announcement_date']" type="date" placeholder="选择日期" value-format="yyyy-MM-dd">
					</el-date-picker>
					<div v-else-if="$check_field('get','announcement_date')">{{form['announcement_date']}}</div>
							</el-form-item>
			</el-col>
								<el-col v-if="$check_field('get','declaration_unit') || $check_field('add','declaration_unit') || $check_field('set','declaration_unit')" :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
							<el-form-item label="申报单位" prop="declaration_unit">
															<el-input id="declaration_unit" v-model="form['declaration_unit']" placeholder="请输入申报单位"
							  v-if="(form['intangible_cultural_heritage_projects_id'] && $check_field('set','declaration_unit')) || (!form['intangible_cultural_heritage_projects_id'] && $check_field('add','declaration_unit'))" :disabled="disabledObj['declaration_unit_isDisabled']"></el-input>
					<div v-else-if="$check_field('get','declaration_unit')">{{form['declaration_unit']}}</div>
											</el-form-item>
			</el-col>
								<el-col v-if="$check_field('get','protecting_units') || $check_field('add','protecting_units') || $check_field('set','protecting_units')" :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
							<el-form-item label="保护单位" prop="protecting_units">
															<el-input id="protecting_units" v-model="form['protecting_units']" placeholder="请输入保护单位"
							  v-if="(form['intangible_cultural_heritage_projects_id'] && $check_field('set','protecting_units')) || (!form['intangible_cultural_heritage_projects_id'] && $check_field('add','protecting_units'))" :disabled="disabledObj['protecting_units_isDisabled']"></el-input>
					<div v-else-if="$check_field('get','protecting_units')">{{form['protecting_units']}}</div>
											</el-form-item>
			</el-col>
								<el-col v-if="$check_field('get','detailed_introduction') || $check_field('add','detailed_introduction') || $check_field('set','detailed_introduction')" :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
							<el-form-item label="详细介绍" prop="detailed_introduction">
											<el-input type="textarea" id="detailed_introduction" v-model="form['detailed_introduction']" placeholder="请输入详细介绍"
						v-if="(form['intangible_cultural_heritage_projects_id'] && $check_field('set','detailed_introduction')) || (!form['intangible_cultural_heritage_projects_id'] && $check_field('add','detailed_introduction'))" :disabled="disabledObj['detailed_introduction_isDisabled']"></el-input>
					<div v-else-if="$check_field('get','detailed_introduction')">{{form['detailed_introduction']}}</div>
							</el-form-item>
			</el-col>
						
	
	
		
		
	
	
	
	
		</el-row>
			<el-col :xs="24" :sm="12" :lg="8" class="el_form_btn_warp">
				<el-form-item v-if="$check_action('/intangible_cultural_heritage_projects/view','set') || $check_action('/intangible_cultural_heritage_projects/view','add') || $check_option('/intangible_cultural_heritage_projects/table','examine')">
					<el-button type="primary" @click="submit()">提交</el-button>
					<el-button @click="cancel()">取消</el-button>
				</el-form-item>
				<el-form-item v-else>
					<el-button @click="cancel()">返回</el-button>
				</el-form-item>
			</el-col>

		</el-form>
	</el-main>
</template>

<script>
	import mixin from "@/mixins/page.js";
	export default {
		mixins: [mixin],
		data() {
			return {
				field: "intangible_cultural_heritage_projects_id",
				url_add: "~/api/intangible_cultural_heritage_projects/add?",
				url_set: "~/api/intangible_cultural_heritage_projects/set?",
				url_get_obj: "~/api/intangible_cultural_heritage_projects/get_obj?",
				url_upload: "~/api/intangible_cultural_heritage_projects/upload?",

				query: {
					"intangible_cultural_heritage_projects_id": 0,
				},

				form: {
								"entry_name":  '', // 项目名称
										"regional_name":  '', // 地区名称
										"project_category":  '', // 项目类别
										"cover_photo":  '', // 封面图片
										"inheritance_personnel": 0, // 传承人员
										"personnel_name":  '', // 人员姓名
										"announcement_date":  '', // 公布日期
										"declaration_unit":  '', // 申报单位
										"protecting_units":  '', // 保护单位
										"detailed_introduction":  '', // 详细介绍
											"intangible_cultural_heritage_projects_id": 0, // ID
													},
				disabledObj:{
								"entry_name_isDisabled": false,
										"regional_name_isDisabled": false,
										"project_category_isDisabled": false,
										"cover_photo_isDisabled": false,
										"inheritance_personnel_isDisabled": false,
										"personnel_name_isDisabled": false,
										"announcement_date_isDisabled": false,
										"declaration_unit_isDisabled": false,
										"protecting_units_isDisabled": false,
										"detailed_introduction_isDisabled": false,
										},

	
									// 地区名称选项列表
				list_regional_name: [""],
	
									// 项目类别选项列表
				list_project_category: [""],
	
			
			
					// 用户列表
				list_user_inheritance_personnel: [],
						// 用户组
				group_user_inheritance_personnel: "",
					
			
			
			
			
		
			}
		},
		methods: {

	
	
			
				/**
			 * 获取地区名称列表
			 */
			async get_list_regional_name() {
				var json = await this.$get("~/api/regional_classification/get_list?");
				if(json.result && json.result.list){
					this.list_regional_name = json.result.list;
				}
				else if(json.error){
					console.error(json.error);
				}
			},
					
			
				/**
			 * 获取项目类别列表
			 */
			async get_list_project_category() {
				var json = await this.$get("~/api/project_classification/get_list?");
				if(json.result && json.result.list){
					this.list_project_category = json.result.list;
				}
				else if(json.error){
					console.error(json.error);
				}
			},
					
						/**
			 * 上传封面图片
			 * @param {Object} param 图片参数
			 */
			upload_cover_photo(param){
									this.uploadFile(param.file, "cover_photo");
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
					/**
			 * 获取传承用户用户组
			 */
			async get_group_user_inheritance_personnel() {
							this.form["inheritance_personnel"] = this.user.user_id;
							var json = await this.$get("~/api/user_group/get_obj?name=传承用户");
				if(json.result && json.result.obj){
					this.group_user_inheritance_personnel = json.result.obj;
				}
				else if(json.error){
					console.error(json.error);
				}
			},
			get_user_session_inheritance_personnel(id){
				var _this = this;
				var user_id = {"user_id":id}
				var url = "~/api/"+_this.group_user_inheritance_personnel.source_table+"/get_obj?"
				this.$get(url, user_id, function(res) {
					if (res.result && res.result.obj) {
						var arr = []
						for (let key in res.result.obj) {
							arr.push(key)
						}
						var arrForm = []
									for (let key in _this.form) {
							arrForm.push(key)
						}
												_this.form["inheritance_personnel"] = id
									_this.disabledObj['inheritance_personnel' + '_isDisabled'] = true
						for (var i=0;i<arr.length;i++){
						  if (arr[i]!=='examine_state' && arr[i]!=='examine_reply') {
							for (var j = 0; j < arrForm.length; j++) {
							  if (arr[i] === arrForm[j]) {
								if (arr[i] !== "inheritance_personnel") {
			                      _this.form[arrForm[j]] = res.result.obj[arr[i]]
			                      _this.disabledObj[arrForm[j] + '_isDisabled'] = true
								  break;
								} else {
								  _this.disabledObj[arrForm[j] + '_isDisabled'] = true
								}
							  }
							}
						  }
						}
					}
				});
			},
					get_user_inheritance_personnel(id){
				var obj = this.list_user_inheritance_personnel.getObj({"user_id":id});
				var ret = "";
				if(obj){
					if(obj.nickname){
						ret = obj.nickname;}
					else{
						ret = obj.username;
					}
				}
				return ret;
			},
			
	
			
	
			
	
			
	
			
	
		
			/**
			 * 获取对象之前
			 * @param {Object} param
			 */
			get_obj_before(param) {
				var form = "";
							
				if(this.form && form){
					Object.keys(this.form).forEach(key => {
						Object.keys(form).forEach(dbKey => {
							// if(dbKey === "charging_standard"){
							// 	this.form['charging_rules'] = form[dbKey];
							// 	this.disabledObj['charging_rules_isDisabled'] = true;
							// };
							if(key === dbKey){
								this.disabledObj[key+'_isDisabled'] = true;
								this.form[key] = form[dbKey]
							}
							if(dbKey === "source_table"){
								this.form['source_table'] = form[dbKey];
							}
							if(dbKey === "source_id"){
								this.form['source_id'] = form[dbKey];
							}
							if(dbKey === "source_user_id"){
								this.form['source_user_id'] = form[dbKey];
							}
						})
					})
				}
														        if (this.form["announcement_date"] && this.form["announcement_date"].indexOf("-")===-1){
          this.form["announcement_date"] = this.$toTime(parseInt(this.form["announcement_date"]),"yyyy-MM-dd")
        }
											$.db.del("form");

				return param;
			},

			/**
			 * 获取对象之后
			 * @param {Object} json
			 * @param {Object} func
			 */
			get_obj_after(json, func){
																																				if(this.form["announcement_date"]=="0000-00-00"){
				  this.form["announcement_date"] = null;
				}
				if(parseInt(this.form["announcement_date"]) > 9999){
					this.form["announcement_date"] = this.$toTime(parseInt(this.form["announcement_date"]),"yyyy-MM-dd")
				}
																			

			},

																																																																												async submit(param, func){
				if (!param) {
					param = this.form;
				}
						
				var pm = this.events("submit_before", Object.assign({}, param)) || param;
				var msg = await this.events("submit_check", pm);
				var ret;
				if (msg) {
					this.$toast(msg, 'danger');
				} else {
																																																																														ret = this.events("submit_main", pm, func);
				}
				return ret;
			},
			
			/**
			 * 提交前验证事件
			 * @param {Object} 请求参数
			 * @return {String} 验证成功返回null, 失败返回错误提示
			 */
						submit_check(param) {
					
																																																													if (!param.announcement_date){
					return "公布日期不能为空";
				}
																																									return null;
			},

			is_view(){
				// var bl = this.user_group == "管理员";
				var bl = false;

				if(!bl){
					bl = this.$check_action('/intangible_cultural_heritage_projects/table','add');
					console.log(bl ? "你有表格添加权限视作有添加权限" : "你没有表格添加权限");
				}
				if(!bl){
					bl = this.$check_action('/intangible_cultural_heritage_projects/table','set');
					console.log(bl ? "你有表格添加权限视作有修改权限" : "你没有表格修改权限");
				}
				if(!bl){
					bl = this.$check_action('/intangible_cultural_heritage_projects/view','add');
					console.log(bl ? "你有视图添加权限视作有添加权限" : "你没有视图添加权限");
				}
				if(!bl){
					bl = this.$check_action('/intangible_cultural_heritage_projects/view','set');
					console.log(bl ? "你有视图修改权限视作有修改权限" : "你没有视图修改权限");
				}
				if(!bl){
					bl = this.$check_action('/intangible_cultural_heritage_projects/view','get');
					console.log(bl ? "你有视图查询权限视作有查询权限" : "你没有视图查询权限");
				}

				console.log(bl ? "具有当前页面的查看权，请注意这不代表你有字段的查看权" : "无权查看当前页，请注意即便有字段查询权限没有页面查询权限也不行");

				return bl;
			},
			/**
			 * 上传文件
			 * @param {Object} param
			 */
			uploadimg(param) {
				this.uploadFile(param.file, "avatar");
			},

		},
		created() {
						this.get_list_regional_name();
						this.get_list_project_category();
									this.get_list_user_inheritance_personnel();
					this.get_group_user_inheritance_personnel();
															},
	}
</script>

<style>
	.avatar-uploader .el-upload {
		border: 1px dashed #d9d9d9;
		border-radius: 6px;
		cursor: pointer;
		position: relative;
		overflow: hidden;
	}

	.avatar-uploader .el-upload:hover {
		border-color: #409EFF;
	}

	.avatar-uploader-icon {
		font-size: 28px;
		color: #8c939d;
		width: 178px;
		height: 178px;
		line-height: 178px;
		text-align: center;
	}

	.avatar {
		width: 178px;
		height: 178px;
		display: block;
	}
	
	.img_multiple{
		overflow: hidden;
	}
	.img_multiple .img_block{
		float: left;
		margin-right: 5px;
		margin-bottom: 5px;
		position: relative;
	}
	.img_multiple .img_block img{
		height: 100px;
		width: auto;
	}
	.img_multiple .img_del{
		position: absolute;
		top: 5px;
		right: 5px;
		width: 20px;
		height: 20px;
		background: #0000008a;
		color: #fff;
		line-height: 20px;
		text-align: center;
		border-radius: 100%;
		cursor: pointer;
	}




	
</style>
