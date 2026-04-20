<template>
  <div class="list_menu diy_menu">
    <router-link
            to="/user_center/index"
            class="menu_item"
            :class="{ selected: this.$route.path=== '/user_center/index'}"
     >
      <span class="left_span">个人首页</span>
      <span class="right_span"><b-icon icon="house-fill"></b-icon></span>
    </router-link>
          <router-link
          to="/ordinary_users/table"
          class="menu_item"
          :class="{ selected: this.$route.path=== '/ordinary_users/table'}"
          v-if="$check_action('/ordinary_users/table','get')">
        <span class="left_span">普通用户</span>
        <span class="right_span"><b-icon icon="house-fill"></b-icon></span>
      </router-link>
              <router-link
          to="/inheriting_users/table"
          class="menu_item"
          :class="{ selected: this.$route.path=== '/inheriting_users/table'}"
          v-if="$check_action('/inheriting_users/table','get')">
        <span class="left_span">传承用户</span>
        <span class="right_span"><b-icon icon="house-fill"></b-icon></span>
      </router-link>
                  <router-link
          to="/project_classification/table"
          class="menu_item"
          :class="{ selected: this.$route.path=== '/project_classification/table'}"
          v-if="$check_action('/project_classification/table','get')">
        <span class="left_span">项目分类</span>
        <span class="right_span"><b-icon icon="house-fill"></b-icon></span>
      </router-link>
              <router-link
          to="/regional_classification/table"
          class="menu_item"
          :class="{ selected: this.$route.path=== '/regional_classification/table'}"
          v-if="$check_action('/regional_classification/table','get')">
        <span class="left_span">地区分类</span>
        <span class="right_span"><b-icon icon="house-fill"></b-icon></span>
      </router-link>
              <router-link
          to="/intangible_cultural_heritage_projects/table"
          class="menu_item"
          :class="{ selected: this.$route.path=== '/intangible_cultural_heritage_projects/table'}"
          v-if="$check_action('/intangible_cultural_heritage_projects/table','get')">
        <span class="left_span">非遗项目</span>
        <span class="right_span"><b-icon icon="house-fill"></b-icon></span>
      </router-link>
              <router-link
          to="/inheritance_personnel/table"
          class="menu_item"
          :class="{ selected: this.$route.path=== '/inheritance_personnel/table'}"
          v-if="$check_action('/inheritance_personnel/table','get')">
        <span class="left_span">传承人员</span>
        <span class="right_span"><b-icon icon="house-fill"></b-icon></span>
      </router-link>
        	<router-link
		to="/logistics_delivery/table"
		class="menu_item"
		:class="{ selected: this.$route.path=== '/logistics_delivery/table'}"
        v-if="$check_action('/logistics_delivery/table')"
	>
		<span class="left_span">订单配送</span>
		<span class="right_span"><b-icon icon="house-fill"></b-icon></span>
	</router-link>
      <router-link
              to="/user/collect"
              class="menu_item"
              :class="{ selected: this.$route.path=== '/collect/list'}"
              v-if="$check_action('/collect/list','get')">
        <span class="left_span">收藏</span>
        <span class="right_span"><b-icon icon="house-fill"></b-icon></span>
  </router-link>
	<router-link
	            to="/comment/table"
	            class="menu_item"
	            :class="{ selected: this.$route.path=== '/comment/table'}"
	            v-if="$check_action('/comment/table','get')">
	      <span class="left_span">评论管理</span>
	      <span class="right_span"><b-icon icon="house-fill"></b-icon></span>
	</router-link>
  </div>
</template>

<script>
import mixin from "@/mixins/page.js";
export default {
  mixins: [mixin],
  data() {
    return {
    };
  },
  mounted() {
  },
  methods: {
  },

  components: {
  },
};
</script>

<style scoped>
.list_menu {
  flex: 0 0 25%;
  border-bottom: 1px solid #eee;
}
.menu_item {
  display: flex;
  justify-content: space-between;
  padding: 15px 20px;
  border: 1px solid #eee;
  border-bottom: none;
  background-color: #fff;
}
.menu_item.selected {
  background-color: #000;
}
.menu_item.selected span {
  color: #fff ;
}
.content {
  flex: 0 0 74%;
}
</style>
