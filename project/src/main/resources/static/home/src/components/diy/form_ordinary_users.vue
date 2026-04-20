<template>
	<div class="diy_form">
			<b-form-group v-if="$check_register_field('add','user_name','/ordinary_users/view')" id="input-group-${idx}" label="用户姓名" label-for="input-${idx}">
			<b-form-input id="input-${idx}" v-model="form['user_name']" type="text" placeholder="请输入用户姓名" trim ></b-form-input>
		</b-form-group>
				<b-form-group v-if="$check_register_field('add','user_age','/ordinary_users/view')" id="input-group-${idx}" label="用户年龄" label-for="input-${idx}">
			<b-form-input id="input-${idx}" v-model="form['user_age']" type="text" placeholder="请输入用户年龄" trim ></b-form-input>
		</b-form-group>
				<b-form-group v-if="$check_register_field('add','user_gender','/ordinary_users/view')" id="input-group-${idx}" label="用户性别" label-for="input-${idx}">
			<b-form-select id="input-${idx}" v-model="form['user_gender']" :options="getDropList(list_user_gender)"></b-form-select>
		</b-form-group>
		</div>
</template>

<script>
	import mixin from "@/mixins/component.js";

	export default {
		mixins: [mixin],
		model: {
			prop: "form",
			event: "change"
		},
		props: {
			form: {
				type: Object,
				default: () => {
					return {

					}
				}
			}
		},
		data(){
			return {
				url_upload: "~/api/ordinary_users/upload?",
									        list_user_gender: "男,女",
							}
		},
		methods: {
			// 下拉数据列表转换
			getDropList(e){
				let resultArr = [];
				if(e){
					let arr = e.split(",");
					arr.forEach(item => {
						resultArr.push({
							value: item,
							text: item
						})
					});
				}
				return resultArr;
			},
									/**
			 * 上传图片
			 * @param {Object} file 文件对象
			 * @param {key} 保存键名
			 */
			uploadFile(file, key = "img") {
				var _this = this;
				var form = new FormData() // FormData 对象
				form.append('file', file[0]) // 文件对象
				this.$upload(this.url_upload, form, function(json) {
					if (json.result) {
						// _this.form[key] = json.result.url;
						_this.$delete(_this.form,key);
						_this.$set(_this.form,key,json.result.url);
						console.log(_this.form[key])
					} else {
						_this.$toast('上传失败！');
					}
				});
			},
			/**
			 * 上传多图
			 * @param {Object} file 文件对象
			 * @param {key} 保存键名
			 */
			uploadFileMultiple(file, key = "img") {
				var _this = this;
				var form = new FormData() // FormData 对象
				form.delete('file')
				for(var i = 0;i < file.length; i ++){
					form.append('file', file[i]) // 文件对象
					this.$upload(this.url_upload, form, function(json) {
						if (json.result) {
							if(key in _this.form){
								_this.form[key].push(json.result.url);
							}else{
								_this.$set(_this.form,key,[json.result.url]);
							}
						}else {
							_this.$toast('上传失败！');
						}
					});
					form.delete('file')
				}
			},
			/**
			 * 删除多图
			 * @param {Object} img 数组下标
			 * @param {key} 保存键名
			 */
			delImg(img, key = "img"){
				var _this = this;
				_this.form[key].splice(img, 1);
			},
		},
    created() {
						    },
	mounted(){
				}
	}
</script>

<style>
.diy_fg.form-group {
    display: flex;
    justify-content: space-between;
}
.diy_in {
    width: 85%;
}
.img_multiple {
	overflow: hidden;
}
.img_multiple .img_block {
	float: left;
	margin-right: 5px;
	margin-bottom: 5px;
	position: relative;
}
.img_multiple .img_block img {
	height: 100px;
	width: auto;
}
.img_multiple .img_del {
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
