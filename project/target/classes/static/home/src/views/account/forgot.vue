<template>
	<div class="page_account forgot" id="account_forgot">
		<div class="warp forgot_warp">
			<div class="container">
				<div class="row row_forgot">
							<div class="t-box">
								<span></span>
								<span></span>
								<span></span>
							</div>
							<!-- 忘记密码 -->
							<div class="forget_container">
								<div class="form_box">
								<b-form class="forms">
									<b-form-group id="input-group-1" label="用户名:" label-for="input-1"
										:state="validation_username" invalid-feedback="账户名长度为在5-16个字符"
										valid-feedback="校验通过">
										<b-form-input id="input-1" v-model="form.username" type="text" placeholder="请输入用户名"
											trim></b-form-input>
									</b-form-group>

									<b-form-group id="input-group-2" label="密码:" label-for="input-2"
										:state="validation_password" invalid-feedback="密码长度为在5-16个字符"
										valid-feedback="校验通过">
										<b-form-input id="input-2" v-model="form.password" type="password"
											placeholder="请输入密码" trim></b-form-input>
									</b-form-group>

									<b-form-group id="input-group-2" label="确认密码:" label-for="input-2"
										:state="validation_confirm_password" invalid-feedback="两次密码不一致"
										valid-feedback="校验通过">
										<b-form-input id="input-2" v-model="confirm_password" type="password"
											placeholder="请再次输入密码" trim></b-form-input>
									</b-form-group>
									<b-form-group id="input-group-2" label="邮箱:" label-for="input-2"
										:state="validation_email" invalid-feedback="邮箱格式example@123.com"
										valid-feedback="">
										<b-form-input id="input-2" v-model="form.email" type="email" placeholder="请输入邮箱"
											trim></b-form-input>
									</b-form-group>

									<b-form-group id="input-group-2" label="验证码:" label-for="input-2"
										:state="validation_code" invalid-feedback="验证码错误">
										<b-input-group>
											<b-form-input id="input-2" v-model="form.code" type="text" placeholder="请输入验证码"
												trim>
											</b-form-input>
											<b-input-group-append>
												<b-button @click="get_code()" variant="success" class="btn"  :style="{ paddingTop: '2px !important' }">
													<span>发送验证码</span></b-button>
											</b-input-group-append>
										</b-input-group>
									</b-form-group>
									
									
								</b-form>
								<div class="btns_bottom fg_bt">
										<button class="btn_item btn" @click="forgot()">找回密码</button>
										<button class="btn_login btn" @click="$router.push('/account/login')">
											<span>返回登录</span>
										</button>
																				
																		</div>
							</div>
								
							</div>
							<div class="b-box">
								<span></span>
								<span></span>
								<span></span>
							</div>

					

				</div>
			</div>
		</div>
	</div>
</template>


<script>
	export default {
		data() {
			return {
				form: {
					username: "",
					password: "",
					email: "",
					code: "",
				},
				confirm_password: "",
				token: "",
			};
		},
		onLoad() {},
		onReady() {},
		methods: {
			get_code(){
				if (!this.form.username){
					this.$toast("用户名不能为空!","error");
					return
				}
				if (!this.form.email){
					this.$toast("邮箱不能为空!","error");
					return
				}
				let email_regular = /^(([^<>()[\]\\.,;:\s@"]+(\.[^<>()[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;
				if(this.form.email && !email_regular.test(this.form.email) ){
					this.$toast("邮箱格式不正确 例：test@test.com","error");
					return
				}
				let _this = this;
				_this.$get('~/api/user/get_obj', {"username": _this.form.username}, function(user_json, status) {
					if (user_json.result && user_json.result.obj) {
						if (user_json.result.obj.email===_this.form.email) {
							// 获取用户状态
							_this.$get('~/api/code_token/send_email', {"email":_this.form.email}, function(json, status) {
								if (json.result) {
									_this.token = json.token;
									_this.$toast('发送成功！', 'success');
								} else if (json.error) {
									_this.$toast(json.error.message,"error");
								} else {
									_this.$toast('服务器连接失败！');
								}
							});
						}else {
							_this.$toast('用户邮箱不正确！', 'error');
						}
					}else {
						_this.$toast('未找到用户！', 'error');
					}
				});
			},

			/**
			 * 忘记密码
			 */
			async forgot() {
				console.log(this.validation_username);
				console.log(this.validation_password);
				console.log(this.validation_confirm_password);
				console.log(this.validation_email);
				console.log(this.validation_code);
				var condition =
						this.validation_username &&
						this.validation_password &&
						this.validation_confirm_password &&
						this.validation_email &&
						this.validation_code;
				if (!condition) {
					this.$toast("输入不合法","error");
					return;
				}

				var form = Object.assign({},this.form);
				let result = await this.$get("~/api/code_token/verify_email_code", {"token": this.token, "code": form.code});
				if (result.result) {
					this.$post("~/api/user/forget_password?", form, (res) => {
						if (res.result) {
							this.$router.push("/account/login");
						} else if (res.error) {
							console.log(res.error);
							this.$toast(res.error.message,"error");
						}
					});
				} else if (result.error) {
					this.$toast(result.error.message, "error");
				} else {
					this.$toast('服务器连接失败！', "error");
				}
			},

			/**
			 * 手动重置表单
			 */
			resetForm() {
				this.$refs.form.resetFields();
			},
		},
		computed: {
			/**
			 * 验证用户名
			 * @return {Boolean}
			 */
			validation_username() {
				var length = this.form.username.length;
				if (!length) {
					return null;
				}
				return length > 4 && length < 17;
			},

			/**
			 * 验证密码
			 * @return {Boolean}
			 */
			validation_password() {
				var length = this.form.password.length;
				if (!length) {
					return null;
				}
				return length > 4 && length < 17;
			},

			/**
			 * 再次验证密码
			 * @return {Boolean}
			 */
			validation_confirm_password() {
				var length = this.confirm_password.length;
				if (!length) {
					return null;
				}
				return this.confirm_password === this.form.password;
			},

			/**
			 * 验证邮箱
			 * @return {Boolean}
			 */
			validation_email() {
				var length = this.form.email.length;
				if (!length) {
					return true;
				}
				return length > 4 && length < 30;
			},

			/**
			 * 验证验证码
			 * @return {Boolean}
			 */
			validation_code() {
				var length = this.form.code.length;
				if (!length) {
					return null;
				}
				return length > 0;
			},
		},
	};
</script>

<style scoped>


	.page_account {
		min-height: 800px;
		display: flex;
		flex-direction: column;
		justify-content: center;
	}

	.flex_cc {
		display: flex;
		justify-content: center;
		align-items: center;
	}

	.forget_container {
		padding: 1rem;
	
	}

	.more_nav {
		display: flex;
		flex-direction: column;
		justify-content: space-around;
		padding: 1rem;
		margin: auto 0;
	}

	.more_nav .btns_bottom {
		display: flex;
		flex-direction: column;
		justify-content: space-between;
	}

	.btn_item {
		cursor: pointer;
		width: 50%;
	
		color: #fff;
		text-align: center;
	
		
	}

	.forgot_nav {
		width:50%;
		cursor: pointer;
	}
</style>
