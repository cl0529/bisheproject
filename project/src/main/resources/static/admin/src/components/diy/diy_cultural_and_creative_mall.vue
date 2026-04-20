<template>
	<el-form ref="form" :rules="rules" :model="form" status-icon label-width="80px">
					<el-col :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
			<el-form-item label="人员姓名" prop="personnel_name">
						<el-input id="personnel_name" v-model="form['personnel_name']" placeholder="请输入人员姓名"
					v-if="(form['cultural_and_creative_mall_id'] && $check_field('set','personnel_name') ) || $check_field('add','personnel_name')"></el-input>
				<div v-else-if="$check_field('get','personnel_name')">{{form['personnel_name']}}</div>
					</el-form-item>
		</el-col>
				<el-col :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
			<el-form-item label="商品规格" prop="product_specifications">
						<el-input id="product_specifications" v-model="form['product_specifications']" placeholder="请输入商品规格"
					v-if="(form['cultural_and_creative_mall_id'] && $check_field('set','product_specifications') ) || $check_field('add','product_specifications')"></el-input>
				<div v-else-if="$check_field('get','product_specifications')">{{form['product_specifications']}}</div>
					</el-form-item>
		</el-col>
	
		<el-col :xs="24" :sm="12" :lg="8" class="el_form_item_warp">
			<el-form-item label="点击数" prop="hits">
				<el-input-number id="hits" v-model="form['hits']"
					v-if="$check_field('set','hits')"></el-input-number>
				<div v-else-if="$check_field('get','hits')">{{form["hits"]}}</div>
			</el-form-item>
		</el-col>


		<el-col :xs="24" :sm="24" :lg="24" style="text-align: center;" class="el_form_btn_warp">
			<el-button type="primary" @click="submit()">提交</el-button>
			<el-button @click="cancel()">取消</el-button>
		</el-col>

	</el-form>
</template>

<script>
	import mixin from "../../mixins/component.js";

	export default {
		mixins: [mixin],
		props:{
			query: {
				type: Object,
				default: function(){
					return {
						"cultural_and_creative_mall_id": 0
					}
				}
			},
			form_goods:{
				type: Object,
				default: function(){
					return {}
				}
			},
			func_check:{
				type: Function,
				default: function(fun){
					console.log("调试输出",fun);
				}
			},
			func_submit:{
				type: Function,
				default: function(fun){
					console.log("调试输出",fun);
				}
			}
		},
		data() {
			return {
				field: "cultural_and_creative_mall_id",
				url_add: "~/api/cultural_and_creative_mall/add?",
				url_set: "~/api/cultural_and_creative_mall/set?",
				url_get_obj: "~/api/cultural_and_creative_mall/get_obj?",
				url_upload: "~/api/cultural_and_creative_mall/upload?",

				form: {
						"seller_customers": 0,
							"personnel_name":  '',
							"product_specifications":  '',
						"hits": 0,
				},

			
				rules: {
					"seller_customers": [     {required: true,message: '卖家用户不能为空'},  ],
					"personnel_name": [     {required: true,message: '人员姓名不能为空'},  ],
					"product_specifications": [     {required: true,message: '商品规格不能为空'},  ],
				}

			}
		},
		methods: {

			/**
			 * 请求列表前
			 * @param {Object} param
			 */
			get_list_before(param) {
				var user_group = this.user.user_group;
				if (user_group !== "管理员") {
					switch (user_group) {
									case "卖家用户":
							param["seller_customers"] = this.user.user_id;
							break;
														}
				}

				return param;
			},

	
	
		
	
		
	
	
			submit(){
				this.func_check(()=>{
					this.$refs["form"].validate((valid)} => {
						if (valid) {
							this.submit();
						} else {
							console.error('error 提交失败!!');
						}
					});
				})
			},

			submit_after(){
				var source_id = this.form_goods.source_id;
				if(source_id){
					this.func_submit();
				}else{
					this.$get('~/api/cultural_and_creative_mall/get_obj?',this.form,(res)=>{
						if(res.result && res.result.obj){
							this.form_goods.source_id = res.result.obj["cultural_and_creative_mall_id"];
							this.func_submit();
						}else{
							console.error(res.error);
						}

					})
				}
			}
		},
		created() {
					}
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
</style>
