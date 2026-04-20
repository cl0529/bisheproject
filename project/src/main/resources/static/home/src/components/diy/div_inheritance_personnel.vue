<template>
	<div class="diy_details_box diy_div_inheritance_personnel">
		<div class="warp diy_warp">
			<div class="container">
					<div class="details_title">详情</div>
				<div class="row row_det">
						<div v-if="this.$store.state.user.user_id" class="btns_add col-12 col-sm-6">
							<div v-if="$check_action('/inheritance_personnel/details','set')">
								<!-- 点赞按钮 -->
								<div
										v-if="state_praise"
										class="praise_btn praise_change"
										@click="change_praise()"
								>
									<b-icon icon="hand-thumbs-up" class="icon icon_change"></b-icon
									><span class="word_change">点赞</span>
								</div>
								<div v-else class="praise_btn" @click="change_praise()">
									<b-icon icon="hand-thumbs-up" class="icon"></b-icon>
									<span>点赞</span>
								</div>
							</div>
							<div v-if="$check_action('/inheritance_personnel/details','set')">
								<!-- 收藏按钮 -->
								<div v-if="state_collect"
									 class="collect_btn collect_change"
									 @click="change_collect()"
								>
									<b-icon icon="heart-fill" class="icon icon_change"></b-icon>
									<span class="word_change">收藏</span>
								</div>
								<div v-else class="collect_btn" @click="change_collect()">
									<b-icon icon="heart" class="icon"></b-icon>
									<span>收藏</span>
								</div>
							</div>
						</div>
						<div class="row_top_wrap">
						<!-- 图片 -->
							<div class="row_1" v-if="imgList.length">
									<div class="diy_img" v-for="(item,index) in imgList" :key="item+index">
									<img :src="$fullUrl(obj[item.name])" />
								</div>
							
							</div>
							<!-- 内容 -->
							<div class="row_2" :class="{flex_row2: !imgList.length}">
							<div class="row_cont" :class="{flex_row2_item: !imgList.length}" v-for="(item,index) in itemList" :key="item+index" v-show="$check_field('get',item.name)">
								<div class="view">
								<div class="diy_title">
									<span>{{item.title}}:</span>
								</div>

								<div class="diy_field diy_uid" :class="{flex_row2_item_text:imgList.length}" v-if="item.type == 'UID'">
									<span>
										{{ get_user_info(item.name,obj[item.name]) }}
									</span>
								</div>
								<div class="diy_field diy_video" :class="{flex_row2_item_text:imgList.length}" v-else-if="item.type == '视频'">
									<router-link :to="'/media/video?filename=' + $fullUrl(obj[item.name])" v-if="obj[item.name]" >
										<span>
											查看视频
										</span>
									</router-link>
								</div>
								<div class="diy_field diy_music" :class="{flex_row2_item_text:imgList.length}" v-else-if="item.type == '音频'">
									<audio v-if="obj[item.name]" style="text-align: left" :src="$fullUrl(obj[item.name])" controls></audio>
								</div>
								<div class="diy_field diy_music" :class="{flex_row2_item_text:imgList.length}" v-else-if="item.type == '文件'">
									<a :href="$fullUrl(obj[item.name])" target="_blank" style="color: rgb(64, 158, 255);">点击下载</a>
								</div>
								<div class="diy_field diy_date" :class="{flex_row2_item_text:imgList.length}" v-else-if="item.type == '日期' || item.type == '日后'">
									<span>
										{{ $toTime(obj[item.name] ,"yyyy-MM-dd") }}
									</span>
								</div>
								<div class="diy_field diy_time" :class="{flex_row2_item_text:imgList.length}" v-else-if="item.type == '时间'">
									<span>
										{{ $toTime(obj[item.name] ,"hh:mm:ss") }}
									</span>
								</div>
								<div class="diy_field diy_datetime" :class="{flex_row2_item_text:imgList.length}" v-else-if="item.type == '日长'">
									<span>
										{{ $toTime(obj[item.name] ,"yyyy-MM-dd hh:mm:ss") }}
									</span>
								</div>
								<div class="diy_field diy_phone" :class="{flex_row2_item_text:imgList.length}"  v-else-if="item.type == '电话' || item.type == '手机'">
									<span>
										{{ obj[item.name] }}
									</span>
								</div>
								<div class="diy_field diy_number" :class="{flex_row2_item_text:imgList.length}" v-else-if="item.type == '数字'">
									<span>
										{{ obj[item.name] }}
									</span>
								</div>
								<div class="diy_field diy_url" :class="{flex_row2_item_text:imgList.length}" v-else-if="item.type == '网址'">
									<div>
										<a :href="obj[item.name]" target="_blank" style="color: rgb(64, 158, 255);">{{ obj[item.name] }}</a>
									</div>
								</div>
								<div class="diy_field diy_multiple_img" :class="{flex_row2_item_text:imgList.length}" v-else-if="item.type == '图集'">
									<div>
										<el-image class="diy_multiple_img_item"
											:src="img" v-for="img in obj[item.name]" :preview-src-list="obj[item.name]">
											<div slot="error" class="image-slot">
												<img src="../../../public/img/error.png" style="width: 90px; height: 90px" />
											</div>
										</el-image>
									</div>
								</div>
								<div class="diy_field diy_desc" :class="{flex_row2_item_text:imgList.length}" v-else-if="item.type == '多文本'">
									<div>
										{{ obj[item.name] }}
									</div>
								</div>
								<!-- <div class="diy_field diy_html"  v-else-if="item.type == '编辑'" v-html="obj[item.name]" >

								</div> -->
								<div class="diy_field diy_text" :class="{flex_row2_item_text:imgList.length}" v-else>
									<span>
										{{ obj[item.name] }}
									</span>
								</div>
							
							</div>
						</div>
															<div class="view praise_wrap">
									<span class="diy_title">点赞数：</span>	<span class="diy_field diy_text">
									{{ praiseLen }}
									</span>
								</div>
																						<div class="view praise_wrap">
									<span class="diy_title">点击数：</span>	<span class="diy_field diy_text">
									{{ hitsLen }}
									</span>
								</div>
																				</div>
					</div>

			<div class="details_btn_wrap">
															</div>
						<!-- 富文本 -->
			<div class="rich_text" v-for="(item,index) in richList" :key="item+index" v-show="$check_field('get',item.name)">
						<div class="view">
						<div class="diy_title">
							<span v-text="item.title"></span>
						</div>
							<div class="diy_html" v-html="obj[item.name]" >

						</div>
						</div>
			</div>

				</div>
			</div>
		</div>
	</div>
</template>

<script>
	import mixin from "@/mixins/component.js";
	import mixinPage from "@/mixins/page.js";

	export default {
		mixins: [mixin,mixinPage],
		components: {

		},
		props: {
			obj: {
				type: Object,
				default: function() {
					return {};
				},
			}
		},
		data() {
			return {
				praiseLen: this.obj["praise_len"],
				hitsLen: this.obj["hits"],
				imgList: [
						{
							title: "封面图片",
							name: "cover_photo",
							type: "图片"
						},
				],
				itemList: [
						{
							title: "项目名称",
							name: "entry_name",
							type: "文本"
						},
						{
							title: "项目类别",
							name: "project_category",
							type: "下寻"
						},
						{
							title: "传承用户",
							name: "inheriting_users",
							type: "UID"
						},
						{
							title: "人员姓名",
							name: "personnel_name",
							type: "文本"
						},
						{
							title: "人员年龄",
							name: "age_of_personnel",
							type: "文本"
						},
						{
							title: "人员性别",
							name: "gender_of_personnel",
							type: "文本"
						},
						{
							title: "出生日期",
							name: "date_of_birth",
							type: "日期"
						},
						{
							title: "申报地区",
							name: "declaration_region",
							type: "文本"
						},
				],
				richList: [
						{
							title: "个人简介",
							name: "personal_profile",
							type: "多文本"
						},
				],
							// 用户列表
				list_user_inheriting_users: [],
												state_praise:false,
				state_collect:false,
			};
		},
		methods: {
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
											get_user_info(name,id){
				var obj = null;
							if (name == 'inheriting_users'){
					obj = this.list_user_inheriting_users.getObj({"user_id":id});
				}
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
				/**
			 * 改变点赞数
			 */
			change_praise(obj) {
				var user_id = this.user.user_id;

				var query = {
					source_table: "inheritance_personnel",
					source_field: "inheritance_personnel_id",
					source_id: this.obj["inheritance_personnel_id"],
					user_id
				};

				var _this = this;

				// 点赞状态
				if (this.state_praise) {
					this.state_praise = false;
					this.$get('~/api/praise/del?', query, (res) => {
						if(res.result){
              _this.praiseLen = parseInt(_this.praiseLen)-1;
              var praise_len = _this.praiseLen;
							this.$post('~/api/inheritance_personnel/set?inheritance_personnel_id=' + _this.obj["inheritance_personnel_id"], {
								praise_len
							}, (res) => {
								if(res.result){
									console.log("添加点赞数状态：" ,res.result);
								}
								else if(res.error){
									console.error(res.error);
								}
							});
							// this.$toast("取消点赞");
							this.$message.success("取消点赞")
						}
						else if (res.error){
							this.$toast(res.error.message);
							console.error(res.error);
						}
					});
				} else {
					this.state_praise = true;
					this.$post('~/api/praise/add?', query, (res) => {
						if (res.result) {
              _this.praiseLen = parseInt(_this.praiseLen)+1;
              var praise_len = _this.praiseLen;
							this.$post('~/api/inheritance_personnel/set?inheritance_personnel_id=' + _this.obj["inheritance_personnel_id"], {
								praise_len
							}, (res) => {
								if(res.result){
									console.log("添加点赞数状态：" ,res.result);
								}
								else if(res.error){
									console.error(res.error);
								}
							});
							// this.$toast("点赞成功");
							this.$message.success("点赞成功")
						}
						else if (res.error) {
							this.$toast(res.error.message);
							console.error(res.error);
						}
					});
				};
			},
      /**
       * 获取点赞
       */
      get_praise() {
        var user_id = this.$store.state.user.user_id;
        this.$get("~/api/praise/count?", {
          source_table: "inheritance_personnel",
          source_field: "inheritance_personnel_id",
          source_id: this.obj.inheritance_personnel_id,
          user_id
        }, (res) => {
          if (res.result || res.result === 0) {
            console.log("sadsfasf"+JSON.stringify(res))
            this.state_praise = res.result ? true : false;
            console.log("点赞状态：" ,this.state_praise);
          }
          else if (res.error){
            this.$toast(res.error.message);
            console.error(res.error);
          }
        })
      },

				/**
			 * 改变收藏状态
			 */
			change_collect() {
				var user_id = this.user.user_id;

				var query = {
					source_table: "inheritance_personnel",
					source_field: "inheritance_personnel_id",
					source_id: this.obj["inheritance_personnel_id"],
					user_id
				};

				// 收藏状态
				if (this.state_collect) {
					this.state_collect = false;
					this.$get('~/api/collect/del?', query, (res) => {
						if(res.result){
							// this.$toast("取消收藏");
							this.$message.success("取消收藏")
						}
						else if (res.error){
							this.$toast(res.error.message);
							console.error(res.error);
						}
					});
				} else {
					this.state_collect = true;
								query.title = this.obj.entry_name
																																							query.img = this.obj.cover_photo
																							this.$post('~/api/collect/add?', query, (res) => {
						if (res.result) {
							// this.$toast("收藏成功");
							this.$message.success("收藏成功")
						}
						else if (res.error) {
							this.$toast(res.error.message);
							console.error(res.error);
						}
					});
				};
			},
      check_collect() {
        var user_id = this.$store.state.user.user_id;
        var inheritance_personnel_id = this.obj.inheritance_personnel_id;
        this.$get('~/api/collect/count?', {
          user_id,
          source_table: "inheritance_personnel",
          source_field: "inheritance_personnel_id",
          source_id: inheritance_personnel_id
        }, (res) => {
          this.state_collect = res.result
        });
      },
	  	
	  },
		created() {
						this.get_list_user_inheriting_users();
								      setTimeout(() => {
        this.get_praise();
        this.check_collect();
      }, 1000);
		},
		watch:{
			obj:{
				deep: true,
				handler(){
					this.praiseLen = this.obj["praise_len"]
					this.hitsLen = this.obj["hits"]
									}
			}
		},
	};
</script>

<style>
.collect_btn,
.praise_btn {
	margin-right: 0.5rem;
	display: flex;
	white-space: nowrap;
	padding: 0.125rem 0.5rem;
	border-radius: 0.25rem;
	color: var(--color_base);
	border: 1px solid var(--color_default);
	cursor: pointer;
}

.icon_change {
	color: var(--color_base);
}

.collect_btn:hover {
	border: 1px solid var(--color_base);
	color: var(color_base);
}

.praise_btn:hover {
	border: 1px solid var(color_base);
	color: var(--color_base);
}
.icon {
	font-size: 15px;
	margin: 3px;
}
.word_change {
	color: var(--color_base);
	font-weight: bold;
}
#app .diy_details_box .warp .container .row .btns_add{
	margin-left: 20px;
}
.btns_add {
	display: flex;
	justify-content: flex-start;
	align-items: center;
	flex-wrap: wrap;
	margin: 0 0.625rem 0.625rem 0;
	font-size: 0.875rem;
	-webkit-tap-highlight-color: rgba(0, 0, 0, 0);
	color: rgba(0, 0, 0, 0.3);
}
.btns_add > * {
	display: flex;
	align-items: center;
	height: 2rem;
}
.diy_multiple_img{
	width: 100%;
}
.diy_multiple_img_item{
	float: left;
	margin: 0 10px 10px 0;
}
.diy_multiple_img_item img{
	height: 100px;
	width: auto;
}
</style>
