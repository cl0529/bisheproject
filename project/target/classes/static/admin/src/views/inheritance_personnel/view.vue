<template>
	<el-main class="bg edit_wrap comtable_e">
		<el-form ref="form" :model="form" status-icon label-width="120px" v-if="is_view()">
		<el-row class="row_ce"> 
							<el-col v-if="$check_field('get','entry_name') || $check_field('add','entry_name') || $check_field('set','entry_name')" :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
							<el-form-item label="项目名称" prop="entry_name">
															<el-input id="entry_name" v-model="form['entry_name']" placeholder="请输入项目名称"
							  v-if="(form['inheritance_personnel_id'] && $check_field('set','entry_name')) || (!form['inheritance_personnel_id'] && $check_field('add','entry_name'))" :disabled="disabledObj['entry_name_isDisabled']"></el-input>
					<div v-else-if="$check_field('get','entry_name')">{{form['entry_name']}}</div>
											</el-form-item>
			</el-col>
								<el-col v-if="$check_field('get','project_category') || $check_field('add','project_category') || $check_field('set','project_category')" :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
							<el-form-item label="项目类别" prop="project_category">
											<el-select id="project_category" v-model="form['project_category']"						v-if="(form['inheritance_personnel_id'] && $check_field('set','project_category')) || (!form['inheritance_personnel_id'] && $check_field('add','project_category'))">
						<el-option v-for="o in list_project_category" :key="o['project_category']" :label="o['project_category']"
							:value="o['project_category']">
						</el-option>
					</el-select>
					<div v-else-if="$check_field('get','project_category')">{{form['project_category']}}</div>
							</el-form-item>
			</el-col>
								<el-col v-if="$check_field('get','inheriting_users') || $check_field('add','inheriting_users') || $check_field('set','inheriting_users')" :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
							<el-form-item label="传承用户" prop="inheriting_users">
																					<div v-if="user_group !== '管理员'">
							{{ get_user_session_inheriting_users(form['inheriting_users']) }}
							<el-select v-if="(form['inheritance_personnel_id'] && $check_field('set','inheriting_users')) || (!form['inheritance_personnel_id'] && $check_field('add','inheriting_users'))" id="inheriting_users" v-model="form['inheriting_users']" :disabled="disabledObj['inheriting_users_isDisabled']">
								<el-option v-for="o in list_user_inheriting_users" :key="o['username']" :label="o['nickname'] + '-' + o['username']"
										   :value="o['user_id']">
								</el-option>
							</el-select>
							<el-select v-else-if="$check_field('get','inheriting_users')" id="inheriting_users" v-model="form['inheriting_users']" :disabled="true">
								<el-option v-for="o in list_user_inheriting_users" :key="o['username']" :label="o['nickname'] + '-' + o['username']"
										   :value="o['user_id']">
								</el-option>
							</el-select>
						</div>
						<el-select v-else id="inheriting_users" v-model="form['inheriting_users']" :disabled="disabledObj['inheriting_users_isDisabled']">
							<el-option v-for="o in list_user_inheriting_users" :key="o['username']" :label="o['nickname'] + '-' + o['username']"
									   :value="o['user_id']">
							</el-option>
						</el-select>
																</el-form-item>
			</el-col>
								<el-col v-if="$check_field('get','personnel_name') || $check_field('add','personnel_name') || $check_field('set','personnel_name')" :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
							<el-form-item label="人员姓名" prop="personnel_name">
															<el-input id="personnel_name" v-model="form['personnel_name']" placeholder="请输入人员姓名"
							  v-if="(form['inheritance_personnel_id'] && $check_field('set','personnel_name')) || (!form['inheritance_personnel_id'] && $check_field('add','personnel_name'))" :disabled="disabledObj['personnel_name_isDisabled']"></el-input>
					<div v-else-if="$check_field('get','personnel_name')">{{form['personnel_name']}}</div>
											</el-form-item>
			</el-col>
								<el-col v-if="$check_field('get','age_of_personnel') || $check_field('add','age_of_personnel') || $check_field('set','age_of_personnel')" :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
							<el-form-item label="人员年龄" prop="age_of_personnel">
															<el-input id="age_of_personnel" v-model="form['age_of_personnel']" placeholder="请输入人员年龄"
							  v-if="(form['inheritance_personnel_id'] && $check_field('set','age_of_personnel')) || (!form['inheritance_personnel_id'] && $check_field('add','age_of_personnel'))" :disabled="disabledObj['age_of_personnel_isDisabled']"></el-input>
					<div v-else-if="$check_field('get','age_of_personnel')">{{form['age_of_personnel']}}</div>
											</el-form-item>
			</el-col>
								<el-col v-if="$check_field('get','gender_of_personnel') || $check_field('add','gender_of_personnel') || $check_field('set','gender_of_personnel')" :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
							<el-form-item label="人员性别" prop="gender_of_personnel">
															<el-input id="gender_of_personnel" v-model="form['gender_of_personnel']" placeholder="请输入人员性别"
							  v-if="(form['inheritance_personnel_id'] && $check_field('set','gender_of_personnel')) || (!form['inheritance_personnel_id'] && $check_field('add','gender_of_personnel'))" :disabled="disabledObj['gender_of_personnel_isDisabled']"></el-input>
					<div v-else-if="$check_field('get','gender_of_personnel')">{{form['gender_of_personnel']}}</div>
											</el-form-item>
			</el-col>
								<el-col v-if="$check_field('get','cover_photo') || $check_field('add','cover_photo') || $check_field('set','cover_photo')" :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
							<el-form-item label="封面图片" prop="cover_photo">
											<el-upload :disabled="disabledObj['cover_photo_isDisabled']" class="avatar-uploader" drag
						accept="image/gif, image/jpeg, image/png, image/jpg" action="" :http-request="upload_cover_photo"
						:show-file-list="false" v-if="(form['inheritance_personnel_id'] && $check_field('set','cover_photo')) || (!form['inheritance_personnel_id'] && $check_field('add','cover_photo'))">
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
								<el-col v-if="$check_field('get','date_of_birth') || $check_field('add','date_of_birth') || $check_field('set','date_of_birth')" :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
							<el-form-item label="出生日期" prop="date_of_birth">
											<el-date-picker :disabled="disabledObj['date_of_birth_isDisabled']" v-if="(form['inheritance_personnel_id'] && $check_field('set','date_of_birth')) || (!form['inheritance_personnel_id'] && $check_field('add','date_of_birth'))" id="date_of_birth"
						v-model="form['date_of_birth']" type="date" placeholder="选择日期" value-format="yyyy-MM-dd">
					</el-date-picker>
					<div v-else-if="$check_field('get','date_of_birth')">{{form['date_of_birth']}}</div>
							</el-form-item>
			</el-col>
								<el-col v-if="$check_field('get','declaration_region') || $check_field('add','declaration_region') || $check_field('set','declaration_region')" :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
							<el-form-item label="申报地区" prop="declaration_region">
															<el-input id="declaration_region" v-model="form['declaration_region']" placeholder="请输入申报地区"
							  v-if="(form['inheritance_personnel_id'] && $check_field('set','declaration_region')) || (!form['inheritance_personnel_id'] && $check_field('add','declaration_region'))" :disabled="disabledObj['declaration_region_isDisabled']"></el-input>
					<div v-else-if="$check_field('get','declaration_region')">{{form['declaration_region']}}</div>
											</el-form-item>
			</el-col>
								<el-col v-if="$check_field('get','personal_profile') || $check_field('add','personal_profile') || $check_field('set','personal_profile')" :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
							<el-form-item label="个人简介" prop="personal_profile">
											<el-input type="textarea" id="personal_profile" v-model="form['personal_profile']" placeholder="请输入个人简介"
						v-if="(form['inheritance_personnel_id'] && $check_field('set','personal_profile')) || (!form['inheritance_personnel_id'] && $check_field('add','personal_profile'))" :disabled="disabledObj['personal_profile_isDisabled']"></el-input>
					<div v-else-if="$check_field('get','personal_profile')">{{form['personal_profile']}}</div>
							</el-form-item>
			</el-col>
						
	
	
		
		
	
	
	
	
		</el-row>
			<el-col :xs="24" :sm="12" :lg="8" class="el_form_btn_warp">
				<el-form-item v-if="$check_action('/inheritance_personnel/view','set') || $check_action('/inheritance_personnel/view','add') || $check_option('/inheritance_personnel/table','examine')">
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
				field: "inheritance_personnel_id",
				url_add: "~/api/inheritance_personnel/add?",
				url_set: "~/api/inheritance_personnel/set?",
				url_get_obj: "~/api/inheritance_personnel/get_obj?",
				url_upload: "~/api/inheritance_personnel/upload?",

				query: {
					"inheritance_personnel_id": 0,
				},

				form: {
								"entry_name":  '', // 项目名称
										"project_category":  '', // 项目类别
										"inheriting_users": 0, // 传承用户
										"personnel_name":  '', // 人员姓名
										"age_of_personnel":  '', // 人员年龄
										"gender_of_personnel":  '', // 人员性别
										"cover_photo":  '', // 封面图片
										"date_of_birth":  '', // 出生日期
										"declaration_region":  '', // 申报地区
										"personal_profile":  '', // 个人简介
											"inheritance_personnel_id": 0, // ID
													},
				disabledObj:{
								"entry_name_isDisabled": false,
										"project_category_isDisabled": false,
										"inheriting_users_isDisabled": false,
										"personnel_name_isDisabled": false,
										"age_of_personnel_isDisabled": false,
										"gender_of_personnel_isDisabled": false,
										"cover_photo_isDisabled": false,
										"date_of_birth_isDisabled": false,
										"declaration_region_isDisabled": false,
										"personal_profile_isDisabled": false,
										},

	
									// 项目类别选项列表
				list_project_category: [""],
	
			
					// 用户列表
				list_user_inheriting_users: [],
						// 用户组
				group_user_inheriting_users: "",
					
			
			
			
			
			
			
		
			}
		},
		methods: {

	
	
			
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
			 * 获取传承用户用户列表
			 */
			async get_list_user_inheriting_users() {
                var json = await this.$get("~/api/user/get_list?user_group=传承用户");
                if(json.result && json.result.list){
                    this.list_user_inheriting_users = json.result.list;
                }
                else if(json.error){
                    console.error(json.error);
                }
			},
					/**
			 * 获取传承用户用户组
			 */
			async get_group_user_inheriting_users() {
							this.form["inheriting_users"] = this.user.user_id;
							var json = await this.$get("~/api/user_group/get_obj?name=传承用户");
				if(json.result && json.result.obj){
					this.group_user_inheriting_users = json.result.obj;
				}
				else if(json.error){
					console.error(json.error);
				}
			},
			get_user_session_inheriting_users(id){
				var _this = this;
				var user_id = {"user_id":id}
				var url = "~/api/"+_this.group_user_inheriting_users.source_table+"/get_obj?"
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
												_this.form["inheriting_users"] = id
									_this.disabledObj['inheriting_users' + '_isDisabled'] = true
						for (var i=0;i<arr.length;i++){
						  if (arr[i]!=='examine_state' && arr[i]!=='examine_reply') {
							for (var j = 0; j < arrForm.length; j++) {
							  if (arr[i] === arrForm[j]) {
								if (arr[i] !== "inheriting_users") {
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
					get_user_inheriting_users(id){
				var obj = this.list_user_inheriting_users.getObj({"user_id":id});
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
			 * 上传封面图片
			 * @param {Object} param 图片参数
			 */
			upload_cover_photo(param){
									this.uploadFile(param.file, "cover_photo");
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
																        if (this.form["date_of_birth"] && this.form["date_of_birth"].indexOf("-")===-1){
          this.form["date_of_birth"] = this.$toTime(parseInt(this.form["date_of_birth"]),"yyyy-MM-dd")
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
																																									if(this.form["date_of_birth"]=="0000-00-00"){
				  this.form["date_of_birth"] = null;
				}
				if(parseInt(this.form["date_of_birth"]) > 9999){
					this.form["date_of_birth"] = this.$toTime(parseInt(this.form["date_of_birth"]),"yyyy-MM-dd")
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
					
																																																																						if (!param.date_of_birth){
					return "出生日期不能为空";
				}
																																return null;
			},

			is_view(){
				// var bl = this.user_group == "管理员";
				var bl = false;

				if(!bl){
					bl = this.$check_action('/inheritance_personnel/table','add');
					console.log(bl ? "你有表格添加权限视作有添加权限" : "你没有表格添加权限");
				}
				if(!bl){
					bl = this.$check_action('/inheritance_personnel/table','set');
					console.log(bl ? "你有表格添加权限视作有修改权限" : "你没有表格修改权限");
				}
				if(!bl){
					bl = this.$check_action('/inheritance_personnel/view','add');
					console.log(bl ? "你有视图添加权限视作有添加权限" : "你没有视图添加权限");
				}
				if(!bl){
					bl = this.$check_action('/inheritance_personnel/view','set');
					console.log(bl ? "你有视图修改权限视作有修改权限" : "你没有视图修改权限");
				}
				if(!bl){
					bl = this.$check_action('/inheritance_personnel/view','get');
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
						this.get_list_project_category();
							this.get_list_user_inheriting_users();
					this.get_group_user_inheriting_users();
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
