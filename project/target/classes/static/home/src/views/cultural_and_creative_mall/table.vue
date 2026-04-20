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
							<!-- 文创商城 -->
							<div><span>文创商城</span></div>
							<table_cultural_and_creative_mall v-if="$check_action('/cultural_and_creative_mall/table','get')"></table_cultural_and_creative_mall>
						</div>
					</div>
				</div>
			</div>
		</div>
	</div>
</template>


<script>
	import list_admin_menu_user from "@/components/diy/list_admin_menu_user.vue";
	import table_cultural_and_creative_mall from "../../components/diy/table_cultural_and_creative_mall.vue";
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
			table_cultural_and_creative_mall,
		},
	};
</script>

<style scoped>
	.container {
		min-height: 800px;
	}
</style>
