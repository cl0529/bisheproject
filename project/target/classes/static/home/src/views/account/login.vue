<template>
	<div class="page_account login" id="account_login">
		<div class="warp login_warp">
			<div class="container">
				<div class="row row_login">

							<!-- 登录 -->
							<div class="form_login">
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
											placeholder="请输入密码" trim autocomplete="off"></b-form-input>
									</b-form-group>

										<!-- 数字验证开始 -->
										<div class="geetest_box">
											<div class="geetest_box_title"><span>验证:</span></div>
											<div class="geetest_box_content">
												<Verification ref="child"></Verification>
											</div>
										</div>
										<!-- 数字验证结束 -->

								</b-form>
									<div class="btns_bottom log_bt">
										<button class="btn_item btn" @click="submit()"><span>登录</span></button>
										<button class="forgot_nav btn" @click="$router.push('/account/forgot')">
											<span>找回密码</span>
										</button>
																			<button class="btn_register btn" @click="$router.push('/account/register')">
											没有存在账户?<span>创建一个账户吧！</span>
										</button>
																		</div>
																	</div>
							</div>



				</div>
			</div>
		</div>
	</div>
</template>

<script>
		import Verification from '@/components/common/verification.vue'
		import {rsaEncrypt} from "@/store/encrypt";
	import mixin from "@/mixins/page.js";

	export default {
		mixins: [mixin],
		data() {
			return {
				allow_user: [
					'管理员'
						,"普通用户"
							,"传承用户"
										],
				form: {
					username: "",
					password: "",
				},
			};
		},
		methods: {
			submit() {
				var _this = this;
				// var condition = this.validation_username && this.validation_password;
				// if (!condition) {
				// 	return this.$message.warning("输入不合法");
				// }
				// 极验开始

						const bool = this.$refs.child.sublim();
						if(!this.validation_username){
						return this.$message.warning("请输入正确账号")
					}else if(!this.validation_password){
						return this.$message.warning("请输入正确密码")
					}
							else if(!bool){
						return
					}
						// 极验结束
				var form = Object.assign({}, this.form);
					form.password = rsaEncrypt(form.password)
					this.$post("~/api/user/login?", form, (res) => {
					if (res.result && res.result.obj) {
						var obj = res.result.obj;
						if(this.allow_user.includes(obj.user_group)){
							$.db.set("token", obj.token);
							_this.$store.commit("user_set", obj);
							
							this.$get_auth(obj.user_group, () => {
								this.$router.push("/");
								location.reload();
							});
							this.$message.success("登录成功！");
						}else{
							this.$message.error("您的账号不支持在用户端登录！");
						}
						
					} else if (res.error) {
						console.log(res.error);
					this.$message.error(res.error.message);
					}
				});
			}
		},
		computed: {
			/**
			 * 验证用户名
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
			 */
			validation_password() {
				var length = this.form.password.length;
				if (!length) {
					return null;
				}
				return length > 4 && length < 17;
			},
		},

		components:{
			Verification,
		}

	};
</script>

<style scoped>
	.page_account {
		min-height: 800px;
		display: flex;
		flex-direction: column;
		justify-content: center;
	}

	button{
		border: 1px solid #32323232;
		border-radius: 7px;
		text-align: center;
    	height: 48px;
    	line-height: 48px;
	}

	button span{
		color:#212121
	}

	.btn_item {
		cursor: pointer;
		width: 50%;

		color: #fff;
		text-align: center;
		height: 48px;
		line-height: 48px;
	}

	.flex_cc {
		display: flex;
		justify-content: center;
		align-items: center;
	}

	.form_login {
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


	.forgot_nav {
		text-align: left;
		font-size: 10px;
		margin-top: 5px;
		cursor: pointer;
	}

	.btn_register{
		margin-top: 5px;
	}

	/*************************************** */
	.sign_in .form .el-input{
		width: 100%;
	}

	/***************极验************************ */
	.geetest_box{
		display: flex;
		margin-bottom: 30px;
		width: 420px;
	}
	.geetest_box_title{
		display: flex;
		width: 100px;
		font-size: 14px;
		text-align: inherit;
		margin-right: 0;
	}
	.geetest_box_title::before{
        content: '*';
    color: #F56C6C;
    margin-right: 4px;
	}
	.geetest_box_content{
	}
	.form_login #input-group-1::before,
	.form_login #input-group-2::before
	{
		content: '*';
		color: #ff3131;
		margin-right: 4px;
	}
</style>
