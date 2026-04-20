<template>
	<div class="page_user comment_table" id="user_address">
		<div class="warp warp_cimment">
			<div class="container">
				<div class="row justify-content-between">
					<div class="col-12 col-md-3">
						<div class="card_menu">
							<!-- 左侧边栏 -->
							<list_admin_menu_user></list_admin_menu_user>
						</div>
					</div>
					<div class="col-12 col-md-9">
						<div class="card_addres View_comments pl-2">
							<div><span>查看评论</span></div>
							<table_comment></table_comment>
						</div>
					</div>
				</div>
			</div>
		</div>
	</div>
</template>


<script>
	import list_admin_menu_user from "@/components/diy/list_admin_menu_user.vue";
	import table_comment from "../../components/diy/comment/table_comment.vue";
	export default {
		data() {
			return {
			};
		},
		mounted() {
		},
		methods: {
		},

		components: {
			list_admin_menu_user,
      table_comment,
		},
	};
</script>

<style scoped>
	.container {
		min-height: 800px;
	}
</style>
