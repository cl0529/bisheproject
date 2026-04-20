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
							<!-- 非遗项目 -->
							<div><span>非遗项目</span></div>
							<view_intangible_cultural_heritage_projects v-if="$check_action('/intangible_cultural_heritage_projects/view','get')"></view_intangible_cultural_heritage_projects>
						</div>
					</div>
				</div>
			</div>
		</div>
	</div>
</template>


<script>
	import list_admin_menu_user from "@/components/diy/list_admin_menu_user.vue";
	import view_intangible_cultural_heritage_projects from "../../components/diy/view_intangible_cultural_heritage_projects.vue";
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
			view_intangible_cultural_heritage_projects,
		},
	};
</script>

<style scoped>
	.container {
		min-height: 800px;
	}
</style>
