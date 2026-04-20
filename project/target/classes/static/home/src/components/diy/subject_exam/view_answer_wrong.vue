<template>
  <el-main class="bg edit_wrap question_edit_wrap">
    <el-form ref="form" :model="form" status-icon label-width="70px">
      <el-row class="row_e">
        <el-col :xs="24" :sm="24" :lg="24" class="el_form_item_warp">
          <el-form-item label="科目" prop="subject_name">
            <el-input v-model="form.subject_name" :readonly="true"></el-input>
          </el-form-item>
        </el-col>

        <el-col :xs="24" :sm="24" :lg="24" class="el_form_item_warp">
          <el-form-item label="类型" prop="type">
            <el-input v-model="form.type" :readonly="true"></el-input>
          </el-form-item>
        </el-col>

        <el-col :xs="24" :sm="24" :lg="24" class="el_form_item_warp">
          <el-form-item label="题目" prop="title">
            <el-input v-model="form.title" :readonly="true"></el-input>
          </el-form-item>
        </el-col>

         <el-col :xs="24" :sm="24" :lg="24" class="el_form_item_warp">
          <el-form-item label="参考答案" prop="answer">
            <el-input
              v-model="form.answer"
              :readonly="true"
            ></el-input>
          </el-form-item>
        </el-col>

        <el-col :xs="24" :sm="24" :lg="24" class="el_form_item_warp">
          <el-form-item label="选项" prop="question_item">
            <el-input
              type="textarea"
              v-model="form.question_item"
              :readonly="true">
            </el-input>
          </el-form-item>
        </el-col>

         <el-col :xs="24" :sm="24" :lg="24" class="el_form_item_warp">
          <el-form-item label="提交答案" prop="answers">
            <el-input
              v-model="form.answers"
              :readonly="true"
            ></el-input>
          </el-form-item>
        </el-col>

      </el-row>
      <el-col :xs="24" :sm="24" :lg="24" class="el_form_btn_warp">
          <el-form-item>
            <el-col
              :xs="24"
              :sm="24"
              :lg="12"
              class="el_form_btn el_form_btn_2"
            >
              <el-button style="width: 100%; float: right" @click="cancel()"
                >取消</el-button
              >
            </el-col>
          </el-form-item>
        </el-col>
    </el-form>
  </el-main>
</template>

<script>
import mixin from "@/mixins/page.js";

export default {
  mixins: [mixin],
  data() {
    return {
      field: "user_answer_id",
      url_add: "~/api/user_answer_wrong/add?",
      url_set: "~/api/user_answer_wrong/set?",
      url_get_obj: "~/api/user_answer_wrong/get_obj?",
      url_upload: "~/api/user_answer_wrong/upload?",

      query: {
        user_answer_id: 0,
      },

      form: {
        user_answer_id: 0,
        type: "",
        answers:"",
		    subject_name:"",
        title: "",
        question_item: "",
        answer: "",
        score: 0,
        exam_id: 0,
      },
      list_subject: [],
    };
  },
  methods: {
    init_exam_id() {
      let exam_id = this.$route.query.exam_id;
      if (exam_id) {
        this.form.exam_id = exam_id;
      }
    },
    submit_check() {},

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
    this.init_exam_id();
    this.get_subject_list();
  },
};
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
  border-color: #409eff;
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

.el-row {
  justify-content: start;
  justify-items: start;
  align-items: start;
  align-content: flex-start;
}
</style>
