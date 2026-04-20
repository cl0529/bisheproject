<template>
	<div class="page_user" id="user_address">
		<div class="warp">
			<div class="container">
				<div class="row justify-content-between">
					<div class="col-12 col-md-3">
						<div class="card_menu">
							<!-- 左侧边栏 -->
							<list_admin_menu_user></list_admin_menu_user>
						</div>
					</div>
					<div class="col-12 col-md-9">
						<div class="card_addres pl-2">
							<!-- 普通用户 -->
							<div><span>普通用户</span></div>
							<view_ordinary_users v-if="$check_action('/ordinary_users/view','get')"></view_ordinary_users>
						</div>
					</div>
				</div>
			</div>
		</div>
	</div>
</template>


<script>
	import list_admin_menu_user from "@/components/diy/list_admin_menu_user.vue";
	import view_ordinary_users from "../../components/diy/view_ordinary_users.vue";
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
			view_ordinary_users,
		},
	};
</script>

<style scoped>
	.container {
		min-height: 800px;
	}
</style>
