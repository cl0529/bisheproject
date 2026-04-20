<template>
  <el-main class="bg table_wrap waron">
    <el-form
      label-position="right"
      :model="query"
      class="form p_4"
      label-width="120"
    >
      <el-row class="rows row1">
        <el-col :xs="24" :sm="24" :lg="8" class="el_form_search_wrap">
          <el-form-item label="科目名称">
            <el-select v-model="query.subject_name">
              <el-option
                v-for="obj in list_subject"
                :key="obj.name"
                :label="obj.name"
                :value="obj.name"
              >
              </el-option>
            </el-select>
          </el-form-item>
        </el-col>

        <el-col :xs="24" :sm="24" :lg="8" class="el_form_search_wrap">
          <el-form-item label="题目名称">
            <el-input v-model="query.title"></el-input>
          </el-form-item>
        </el-col>

       
      </el-row>
      <el-row class="rows row2">
        <el-col :xs="24" :sm="24" :lg="24" class="search_btn_wrap">
          <el-col :xs="24" :sm="12" :lg="12" class="search_btn_1">
            
            <el-button
                type="primary"
                @click="search()"
                class="search_btn_find"
                >查询</el-button
              >
              <el-button @click="reset()" class="search_btn_reset"
                >重置</el-button
              >
              <el-button
                v-if="
                  $check_action('/user_answer_wrong/table', 'del')
                "
                class="float-right search_btn_del"
                type="danger"
                @click="delInfo()"
                >删除</el-button
              >
           
          </el-col>
          
        </el-col>
      </el-row>
     
    </el-form>
    <el-table
      border
      :data="list"
      @selection-change="selectionChange"
      @sort-change="$sortChange"
      style="width: 100%"
      stripe
    >
      <el-table-column fixed type="selection" tooltip-effect="dark" width="50">
      </el-table-column>

      <el-table-column
        fixed
        prop="subject_name"
        label="科目名称"
        min-width="180"
      >
      </el-table-column>

      <el-table-column fixed prop="type" label="类型" min-width="180">
      </el-table-column>

      <el-table-column fixed prop="title" label="题目" min-width="180">
      </el-table-column>

      <el-table-column fixed prop="nickname" label="提交人" min-width="180">
      </el-table-column>

      <el-table-column
        sortable
        prop="create_time"
        label="提交时间"
        min-width="200"
      >
        <template slot-scope="scope">
          {{ $toTime(scope.row["create_time"], "yyyy-MM-dd hh:mm:ss") }}
        </template>
      </el-table-column>

      <!-- 操作 -->
      <el-table-column fixed="right" label="操作" width="180">
        <template slot-scope="scope">
          <router-link
            class="e-button el-button--small is-plain el-button--primary"
            v-if="
              $check_action('/user_answer_wrong/view', 'get')
            "
            :to="'./answer_wrong_view?' + field + '=' + scope.row[field]"
            size="small"
          >
            <span>详情</span>
          </router-link>
        </template>
      </el-table-column>
      <!-- /操作 -->
    </el-table>

    <!-- 分页器 -->
    <div class="mt text_center">
      <el-pagination
        @size-change="handleSizeChange"
        @current-change="handleCurrentChange"
        :current-page="query.page"
        :page-sizes="[7, 10, 30, 100]"
        :page-size="query.size"
        layout="total, sizes, prev, pager, next, jumper"
        :total="count"
      >
      </el-pagination>
    </div>
    <!-- /分页器 -->
  </el-main>
</template>

<script>
import mixin from "@/mixins/page.js";

export default {
  mixins: [mixin],
  data() {
    return {
      // 获取数据地址
      url_get_list: "~/api/user_answer_wrong/get_list",
      url_del: "~/api/user_answer_wrong/del?",

      // 字段ID
      field: "user_answer_id",

      // 查询
      query: {
        size: 10,
        page: 1,
        title: "",
        orderby: "create_time desc",
        nickname:
          this.$store.state.user.user_group == "管理员"
            ? ""
            : this.$store.state.user.nickname,
      },

      // 数据
      list: [],

      list_is: [
        { name: "否", value: 0 },
        { name: "是", value: 1 },
      ],
      answered_list: [],

      list_subject:[]
    };
  },
  computed: {},
  methods: {
    deleteRow(index, rows) {
      rows.splice(index, 1);
    },
    get_list_after() {
      for (let i = 0; i < this.list.length; i++) {
        this.get_user_answer_state(this.list[i].exam_id);
      }
    },
    get_user_answer_state(exam_id) {
      let _this = this;
      this.$get(
        "~/api/user_answer/get_obj?user_id=" +
          this.$store.state.user.user_id +
          "&exam_id=" +
          exam_id
      ).then((res) => {
        console.log(res);
        if (res.result && res.result.obj != null) {
          _this.$set(_this.answered_list, exam_id, true);
        } else {
          _this.$set(_this.answered_list, exam_id, false);
        }
      });
    },
    table_class({ row, column, rowIndex, columnIndex }) {
      return "table_class";
    },

    /**
     * 获取科目列表
     */
    async get_subject_list() {
      var json = await this.$get("~/api/subject/get_list?");
      if (json.result) {
        this.list_subject = json.result.list;
      } else if (json.error) {
        console.error(json.error);
      }
    },
  },
  created() {
    this.get_subject_list();
  },
};
</script>

<style type="text/css">
</style>
