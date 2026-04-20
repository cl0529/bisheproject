<template>
	<el-main class="bg edit_wrap comtable_e">
		<el-form ref="form" :model="form" status-icon label-width="120px" v-if="is_view()">
		<el-row class="row_ce"> 
			<el-col :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
				<el-form-item label="头像" prop="avatar">
					<el-upload class="avatar-uploader" drag accept="image/gif, image/jpeg, image/png, image/jpg"
						action="" :http-request="uploadimg" :show-file-list="false">
						<img v-if="form.avatar" :src="$fullUrl(form.avatar)" class="avatar">
						<i v-else class="el-icon-plus avatar-uploader-icon"></i>
					</el-upload>
				</el-form-item>
			</el-col>

			<el-col :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
				<el-form-item label="账号" prop="username">
			        <span v-if="obj.username">{{obj.username}}</span>
					<el-input v-else v-model="form.username" placeholder="请输入账号"></el-input>
				</el-form-item>
			</el-col>

			<el-col v-if="!obj.password" :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
				<el-form-item label="密码" prop="password">
					<!--<span v-if="obj.password">{{obj.password}}</span>-->
					<!--<el-input v-else type="password" v-model="form.password" placeholder="请输入密码" show-password></el-input>-->
					<el-input type="password" v-model="form.password" placeholder="请输入密码" show-password></el-input>
				</el-form-item>
			</el-col>

			<el-col :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
				<el-form-item label="昵称" prop="nickname">
					<el-input v-model="form.nickname" placeholder="请输入昵称"></el-input>
				</el-form-item>
			</el-col>


			<el-col :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
				<el-form-item label="邮箱" prop="email">
					<el-input type="email" v-model="form.email" placeholder="请输入邮箱"></el-input>
				</el-form-item>
			</el-col>



			<el-col :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
				<el-form-item label="状态" prop="state">
					<el-select :disabled="user_group!=='管理员'" v-model="form.state" placeholder="请选择">
						<el-option v-for="group in list_user_state" :key="group.value" :label="group.name"
							:value="group.value">
						</el-option>
					</el-select>
				</el-form-item>
			</el-col>

	
	
						<el-col v-if="$check_field('get','personnel_name') || $check_field('add','personnel_name') || $check_field('set','personnel_name')" :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
				<el-form-item label="人员姓名" prop="personnel_name">
												<el-input id="personnel_name" v-model="form_sub['personnel_name']" placeholder="请输入人员姓名"
							  v-if="(form_sub['inheriting_users_id'] && $check_field('set','personnel_name')) || (!form_sub['inheriting_users_id'] && $check_field('add','personnel_name'))"></el-input>
					<div v-else-if="$check_field('get','personnel_name')">{{form_sub['personnel_name']}}</div>
											</el-form-item>
			</el-col>
								<el-col v-if="$check_field('get','age_of_personnel') || $check_field('add','age_of_personnel') || $check_field('set','age_of_personnel')" :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
				<el-form-item label="人员年龄" prop="age_of_personnel">
												<el-input id="age_of_personnel" v-model="form_sub['age_of_personnel']" placeholder="请输入人员年龄"
							  v-if="(form_sub['inheriting_users_id'] && $check_field('set','age_of_personnel')) || (!form_sub['inheriting_users_id'] && $check_field('add','age_of_personnel'))"></el-input>
					<div v-else-if="$check_field('get','age_of_personnel')">{{form_sub['age_of_personnel']}}</div>
											</el-form-item>
			</el-col>
								<el-col v-if="$check_field('get','gender_of_personnel') || $check_field('add','gender_of_personnel') || $check_field('set','gender_of_personnel')" :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
				<el-form-item label="人员性别" prop="gender_of_personnel">
								<el-select id="gender_of_personnel" v-model="form_sub['gender_of_personnel']"
						v-if="(form_sub['inheriting_users_id'] && $check_field('set','gender_of_personnel')) || (!form_sub['inheriting_users_id'] && $check_field('add','gender_of_personnel'))">
						<el-option v-for="o in list_gender_of_personnel" :key="o" :label="o" :value="o">
						</el-option>
					</el-select>
					<div v-else-if="$check_field('get','gender_of_personnel')">{{form_sub['gender_of_personnel']}}</div>
							</el-form-item>
			</el-col>
								<el-col v-if="$check_field('get','home_address') || $check_field('add','home_address') || $check_field('set','home_address')" :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
				<el-form-item label="家庭住址" prop="home_address">
												<el-input id="home_address" v-model="form_sub['home_address']" placeholder="请输入家庭住址"
							  v-if="(form_sub['inheriting_users_id'] && $check_field('set','home_address')) || (!form_sub['inheriting_users_id'] && $check_field('add','home_address'))"></el-input>
					<div v-else-if="$check_field('get','home_address')">{{form_sub['home_address']}}</div>
											</el-form-item>
			</el-col>
								<el-col v-if="$check_field('get','id_picture') || $check_field('add','id_picture') || $check_field('set','id_picture')" :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
				<el-form-item label="证件图片" prop="id_picture">
								<el-upload :disabled="disabledObj['id_picture_isDisabled']" class="avatar-uploader" drag
						accept="image/gif, image/jpeg, image/png, image/jpg" action="" :http-request="upload_id_picture"
						:show-file-list="false" v-if="(form_sub['inheriting_users_id'] && $check_field('set','id_picture')) || (!form_sub['inheriting_users_id'] && $check_field('add','id_picture'))">
						<img id="id_picture" v-if="form_sub['id_picture']" :src="$fullUrl(form_sub['id_picture'])" class="avatar">
						<i v-else class="el-icon-plus avatar-uploader-icon"></i>
					</el-upload>
					<el-image v-else-if="$check_field('get','id_picture')" style="width: 100px; height: 100px"
						:src="$fullUrl(form_sub['id_picture'])" :preview-src-list="[$fullUrl(form_sub['id_picture'])]">
						<div slot="error" class="image-slot">
							<img src="../../../public/img/error.png" style="width: 90px; height: 90px" />
						</div>
					</el-image>
							</el-form-item>
			</el-col>
			
					<el-col :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
				<el-form-item label="审核状态" prop="examine_state">
					<el-select id="examine_state" v-model="form_sub['examine_state']"
						v-if="(form_sub['examine_state'] && $check_examine()) || (!form_sub['examine_state'] && $check_examine())" :disabled="true">
						<el-option key="未审核" label="未审核" value="未审核"></el-option>
						<el-option key="已通过" label="已通过" value="已通过"></el-option>
						<el-option key="未通过" label="未通过" value="未通过"></el-option>
					</el-select>
					<div v-else>{{form_sub["examine_state"]}}</div>
				</el-form-item>
			</el-col>
		
		</el-row>
			<el-col :xs="24" :sm="12" :lg="8" class="el_form_btn_warp">
				<el-form-item v-if="$check_action('/inheriting_users/view','set') || $check_action('/inheriting_users/view','add') || $check_option('/inheriting_users/table','examine')">
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
				field: "user_id",
				url_add: "~/api/user/register?",
				url_set: "~/api/user/set?",
				url_get_obj: "~/api/user/get_obj?",
				url_upload: "~/api/user/upload?",

				group_table: "inheriting_users",
				is_password: true,

				query: {
					"user_id": 0,
				},

				obj: {
					user_id: 0,
					username: '',
					nickname: '',
					password: '',
					avatar: '',
					// phone: '',
					email: '',
						user_group: "传承用户",
					// phone_state: 0,
					// email_state: 0,
					state: 1,
					},

				form: {
					user_id: 0,
				    username: '',
					nickname: '',
				    password: '',
					avatar: '',
					// phone: '',
					email: '',
						user_group: "传承用户",
					// phone_state: 0,
					// email_state: 0,
					state: 1,
					},

				disabledObj:{
								"personnel_name_isDisabled": false,
										"age_of_personnel_isDisabled": false,
										"gender_of_personnel_isDisabled": false,
										"home_address_isDisabled": false,
										"id_picture_isDisabled": false,
							},

				form_sub: {
								"personnel_name":  '', // 人员姓名
										"age_of_personnel":  '', // 人员年龄
										"gender_of_personnel":  '', // 人员性别
										"home_address":  '', // 家庭住址
										"id_picture":  '', // 证件图片
									"examine_state": "未审核",
						    "user_id": 0,
					"inheriting_users_id": 0 // ID
				},

				list_state: [{
					value: 0,
					name: "未认证"
				}, {
					value: 1,
					name: "审核中"
				}, {
					value: 2,
					name: "已认证"
				}],

				list_user_state: [{
					value: 1,
					name: "可用"
				}, {
					value: 2,
					name: "异常"
				}, {
					value: 3,
					name: "已冻结"
				}, {
					value: 4,
					name: "已注销"
				}],
	
	
			
									// 人员性别选项列表
				list_gender_of_personnel: ['男','女'],
	
			
			
		
			}
		},
		methods: {

	
	
			
	
			
	
			
	
						/**
			 * 上传证件图片
			 * @param {Object} param 图片参数
			 */
			upload_id_picture(param){
						let _this = this;
				var form = new FormData() // FormData 对象
				form.append('file', param.file) // 文件对象
				this.$upload(this.url_upload, form, function(json) {
					if (json.result) {
												_this.form_sub['id_picture'] = json.result.url;
											} else {
						_this.$toast('上传失败！');
					}
				});
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
														$.db.del("form");

				return param;
			},

			/**
			 * 获取对象之后
			 * @param {Object} json
			 * @param {Object} func
			 */
			get_obj_after(json, func){
				var user = json.result.obj;
				this.is_password = user.password ? false : true;
				var user_id = user.user_id;
				this.$get("~/api/" + this.group_table + "/get_obj?" ,{user_id} ,(res)=>{
					if(res.result && res.result.obj){
						var o = res.result.obj;
						delete o["create_time"];
						delete o["update_time"];
						this.form_sub = res.result.obj;
																															}else if(res.error){
						console.log(res.error);
						console.log("获取不到相关信息");
					}
				})
																									

			},

			delImg(img, key = "img"){
				var _this = this;
				_this.form_sub[key].splice(img, 1);
			},
			async submit(param, func){
				if (!param) {
					param = this.form;
				}
				param.avatar = param.avatar == "" ? "/api/upload/default_avatar.jpg" : param.avatar;
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

			// 提交前校验
			async submit_check(param){
				var ret = null;

				var email_regular = /^(([^<>()[\]\\.,;:\s@"]+(\.[^<>()[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;
				// var phone_regular = /^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\d{8}$/;

				// var {username ,password ,nickname ,user_group ,email ,phone} = param;
				var {username ,password ,nickname ,user_group ,email} = param;

				var confirm_password = this.confirm_password;

				console.log("表单校验username ,password ,email ,nickname ,user_group" ,username ,password ,email ,nickname ,user_group);

				if(!username){
					ret = "账号不能为空";
				}
				else if(username.length > 16 || username.length < 5){
					ret = "账号长度应为5到16个字符之间！";
				}
				else if(!password){
					ret = "密码不能为空!";
				}
				else if(this.is_password && (password.length > 16 || password.length < 5)){
					ret = "密码长度应为5到16个字符之间！";
				}
				else if(nickname && nickname.length > 12 || nickname.length < 2){
					ret = "昵称长度应为2个字符到12个字符之间";
				}
				else if(email && !email_regular.test(email)){
					ret = "请输入正确的邮箱地址 例：test@test.com!";
				}
				// else if(phone && !phone_regular.test(phone)){
				// 	ret = "请输入正确的手机号码 例：18955552312!";
				// }
				else if(!user_group){
					ret = "请选择用户组!";
				}

				var p = {"username": param.username};

				var form_sub = Object.assign({} ,this.form_sub);
																																																																								
				return ret;
			},

			// 提交成功后
			submit_after(json,func){
				var form = Object.assign({} ,this.form);
				delete form.password;
				console.log("查询表单form",form)
				this.get_register(form);
			},

			/**
			 * 获取注册表信息
			 * @param {Object} form
			 * @param {Object} table
			 */
			get_register(form){
				var form_sub = this.form_sub;
				this.$get("~/api/user/get_obj?",form,(res)=>{
					console.log("注册表信息res",res);
					if(res.result && res.result.obj){
						form_sub.user_id = res.result.obj.user_id;
						this.submit_sub(form_sub);
					}else if(res.error){
						console.error(res.error);
						this.$toast(res.error.message,"error");
					}
				})
			},

			// 提交附加信息
			submit_sub(form_sub){
        form_sub = this.events("submit_before", Object.assign({}, form_sub)) || form_sub;
																															if(form_sub["inheriting_users_id"]){
					// 提交事件
					this.$post("~/api/" + this.group_table + "/set?inheriting_users_id=" + form_sub["inheriting_users_id"],form_sub,(res)=>{
						console.log("提交结果：" ,res)
						if(res.result){
							this.$toast("修改成功!" ,"success");
							this.$router.go(-2);
						}else if(res.error){
							console.error(res.error);
							this.$toast(res.error.message,"error");
						}
					});
				}
				else{
					// 提交事件
					this.$post("~/api/" + this.group_table + "/add?",form_sub,(res)=>{
						console.log("提交结果：" ,res)
						if(res.result){
							this.$toast("添加成功!" ,"success");
							this.$router.go(-2);
						}else if(res.error){
							var user_id = form_sub["user_id"];
							this.$get("~/api/user/del?",{user_id});
							console.error(res.error);
							this.$toast(res.error.message,"error");
						}
					});
				}
			},

			is_view(){
				// var bl = this.user_group == "管理员";
				var bl = false;

				if(!bl){
					bl = this.$check_action('/inheriting_users/table','add');
					console.log(bl ? "你有表格添加权限视作有添加权限" : "你没有表格添加权限");
				}
				if(!bl){
					bl = this.$check_action('/inheriting_users/table','set');
					console.log(bl ? "你有表格添加权限视作有修改权限" : "你没有表格修改权限");
				}
				if(!bl){
					bl = this.$check_action('/inheriting_users/view','add');
					console.log(bl ? "你有视图添加权限视作有添加权限" : "你没有视图添加权限");
				}
				if(!bl){
					bl = this.$check_action('/inheriting_users/view','set');
					console.log(bl ? "你有视图修改权限视作有修改权限" : "你没有视图修改权限");
				}
				if(!bl){
					bl = this.$check_action('/inheriting_users/view','get');
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
