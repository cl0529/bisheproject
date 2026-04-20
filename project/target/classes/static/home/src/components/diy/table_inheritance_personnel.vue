<template>
  <el-main class="bg table_wrap">
    <el-form label-position="right" :model="query" class="form p_4" label-width="120">
      <el-row class="rows row1">

          
                                                                                              <el-col :xs="24" :sm="24" :lg="8" class="el_form_search_wrap">
                      <el-form-item label="项目类别">
                                                      <el-select v-model="query.project_category">
                                                                  <el-option v-for="o in list_project_category" :key="o.project_category" :label="o.project_category"
                                             :value="o.project_category">
                                  </el-option>
                                                            </el-select>
                                                </el-form-item>
                    </el-col>
                                                                                                      <el-col :xs="24" :sm="24" :lg="8" class="el_form_search_wrap">
                      <el-form-item label="人员姓名">
                                                      <el-input v-model="query.personnel_name"></el-input>
                                                </el-form-item>
                    </el-col>
                                                                                                                                                                                                                                                                </el-row>
      <el-row class="rows row2">
      	<el-col :xs="24" :sm="24" :lg="24" class="search_btn_wrap search_btns">
         <el-col :xs="24" :sm="10" :lg="8" class="search_btn_1 search_btn_wrap_1 btns">
                              <el-button type="primary" @click="search()" class="search_btn_find">查询</el-button>
                <el-button @click="reset()" style="margin-right: 74px;" class="search_btn_reset">重置</el-button>
                                                      <el-button v-if="$check_action('/inheritance_personnel/table','add') || $check_action('/inheritance_personnel/view','add')" @click="$router.push('./view?')" class="add">添加</el-button>
            <el-button v-if="$check_action('/inheritance_personnel/table','del') || $check_action('/inheritance_personnel/view','del')" class="search_btn_del" type="danger" @click="delInfo()">删除</el-button>
                        </el-col>
       
        </el-col>
      </el-row>
    </el-form>
	    <el-table :data="list" @selection-change="selectionChange" @sort-change="$sortChange" style="width: 100%" id="dataTable">
	                <el-table-column fixed type="selection" tooltip-effect="dark" width="55">
            </el-table-column>
                                                  <el-table-column prop="entry_name" @sort-change="$sortChange" label="项目名称"                                v-if="$check_field('get','entry_name')" min-width="200">
                                </el-table-column>
                                              <el-table-column prop="project_category" @sort-change="$sortChange" label="项目类别"                                v-if="$check_field('get','project_category')" min-width="200">
                                </el-table-column>
                                              <el-table-column prop="inheriting_users" @sort-change="$sortChange" label="传承用户"                                v-if="$check_field('get','inheriting_users')" min-width="200">
                                      <template slot-scope="scope">
                      {{ get_user_inheriting_users(scope.row['inheriting_users']) }}
                    </template>
                                </el-table-column>
                                              <el-table-column prop="personnel_name" @sort-change="$sortChange" label="人员姓名"                                v-if="$check_field('get','personnel_name')" min-width="200">
                                </el-table-column>
                                              <el-table-column prop="age_of_personnel" @sort-change="$sortChange" label="人员年龄"                                v-if="$check_field('get','age_of_personnel')" min-width="200">
                                </el-table-column>
                                              <el-table-column prop="gender_of_personnel" @sort-change="$sortChange" label="人员性别"                                v-if="$check_field('get','gender_of_personnel')" min-width="200">
                                </el-table-column>
                                              <el-table-column prop="cover_photo" @sort-change="$sortChange" label="封面图片"                                v-if="$check_field('get','cover_photo')" min-width="200">
                                      <template slot-scope="scope">
                      <el-image style="width: 100px; height: 100px" :src="$fullUrl(scope.row['cover_photo'])"
                                :preview-src-list="[$fullUrl(scope.row['cover_photo'])]">
                        <div slot="error" class="image-slot">
                          <img src="../../../public/img/error.png" style="width: 90px; height: 90px" />
                        </div>
                      </el-image>
                    </template>
				                </el-table-column>
                                              <el-table-column prop="date_of_birth" @sort-change="$sortChange" label="出生日期"                                v-if="$check_field('get','date_of_birth')" min-width="200">
                                      <template slot-scope="scope">
                      {{ $toTime(scope.row["date_of_birth"],"yyyy-MM-dd") }}
                    </template>
                                </el-table-column>
                                              <el-table-column prop="declaration_region" @sort-change="$sortChange" label="申报地区"                                v-if="$check_field('get','declaration_region')" min-width="200">
                                </el-table-column>
                                              <el-table-column prop="personal_profile" @sort-change="$sortChange" label="个人简介"                                v-if="$check_field('get','personal_profile')" min-width="200">
                                </el-table-column>
                    				        
        
                
      <el-table-column sortable prop="create_time" label="创建时间" min-width="200">
        <template slot-scope="scope">
          {{ $toTime(scope.row["create_time"],"yyyy-MM-dd hh:mm:ss") }}
        </template>
      </el-table-column>

      <el-table-column sortable prop="update_time" label="更新时间" min-width="200">
        <template slot-scope="scope">
          {{ $toTime(scope.row["update_time"],"yyyy-MM-dd hh:mm:ss") }}
        </template>
      </el-table-column>

        
        
        
        
        

                <el-table-column fixed="right" label="操作" min-width="120" v-if="$check_action('/inheritance_personnel/table','set') || $check_action('/inheritance_personnel/view','set') || $check_action('/inheritance_personnel/view','get')
					 ">
        

      <template slot-scope="scope">
        <router-link class="el-button el-button--small is-plain el-button--success" style="margin: 5px !important;"
                     v-if="$check_action('/inheritance_personnel/table','set') || $check_action('/inheritance_personnel/view','set') || $check_action('/inheritance_personnel/view','get')"
                                 :to="'./view?' + field + '=' + scope.row[field]"
                                 size="small">
          <span>详情</span>
        </router-link>
                                                                      <router-link v-if="$check_comment('/inheritance_personnel/table')" class="el-button el-button--small is-plain el-button--primary"
                             :to="'../comment/table?size=10&page=1&source_table=inheritance_personnel&source_field=' + field + '&source_id=' + scope.row[field]" size="small">
                  查看评论
                </router-link>
                                  		  		        </template>
    </el-table-column>

    </el-table>

    <!-- 分页器 -->
    <div class="mt text_center">
      <el-pagination @size-change="handleSizeChange" @current-change="handleCurrentChange"
                     :current-page="query.page" :page-sizes="[7, 10, 30, 100]" :page-size="query.size"
                     layout="total, sizes, prev, pager, next, jumper" :total="count">
      </el-pagination>
    </div>
    <!-- /分页器 -->
                                                                                                                                                                                  
    <div class="modal_wrap" v-if="showModal">
      <div class="modal_box">
        <!-- <div class="modal_box_close" @click="closeModal">X</div> -->
        <p class="modal_box_title">重要提醒</p>
        <p class="modal_box_text">当前有数据达到预警值！</p>
        <p class="modal_box_text">{{ message }}</p>
        <div class="btn_box">
          <span @click="closeModal">取消</span>
          <span @click="closeModal">确定</span>
        </div>
      </div>
    </div>


  </el-main>
</template>
<script>
  import mixin from "@/mixins/page.js";
  
  export default {
    mixins: [mixin],
    data() {
      return {
        // 弹框
        showModal: false,
        // 获取数据地址
        url_get_list: "~/api/inheritance_personnel/get_list?like=0",
        url_del: "~/api/inheritance_personnel/del?",

        // 字段ID
        field: "inheritance_personnel_id",
        // 查询
        query: {
          "size": 7,
          "page": 1,
                                                                                                  "project_category": "",
                                                                                                                                      "personnel_name": "",
                                                                                                                                                                                                                                                                  "login_time": "",
          "create_time": "",
          "orderby": `create_time desc`
        },

                  // 数据
        list: [],
                                                                                                                                                                                                                                                                                                                                                                      // 项目类别列表
                list_project_category: [""],
                                                                                                    // 用户列表
                list_user_inheriting_users: [],
                                                                                                                                                                                                                                                                                                  		  		  message: '',
      }
    },
    methods: {
      // 关闭弹框
      closeModal(){
        this.showModal = false;
      },
        		  /**
		   * @description 获取到列表事件
		   * @param {Object} res 响应结果
		   */
		  get_list_after: function get_list_after(res, func, url) {
		  	let _this = this
																																		  },
        
                  get_list_before(param){
            var user_group = this.$store.state.user.user_group;
            if(user_group != "管理员"){
                                  let sqlwhere = "(";
                                                                                                                                                              if(user_group=="传承用户"){
                            sqlwhere+= "inheriting_users = " + this.$store.state.user.user_id + " or ";
                          }
                                                                                                                                                                                                                                                                                                                                                                                  if (sqlwhere.length>1){
                    sqlwhere = sqlwhere.substr(0,sqlwhere.length-4);
                    sqlwhere += ")";
                    param["sqlwhere"] = sqlwhere;
                  }
                            }
            return param;
          },
        
                                                                  /**
               * 获取项目类别列表
               */
              async get_list_project_category() {
                var json = await this.$get("~/api/project_classification/get_list?");
                if(json.result){
                  this.list_project_category = json.result.list;
                }else if (json.error){
                  console.log(json.error);
                }
              },
                                                                                                                                                                                                                                                                                                        
        
        
        
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

              get_user_inheriting_users(id){
                var obj = this.list_user_inheriting_users.getObj({"user_id":id});
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
                                                                                                                                                                                                                                                                                        		      deleteRow(index, rows) {
        rows.splice(index, 1);
      },

    },
	    created() {
                                                                          // 初始化项目类别列表
              this.get_list_project_category();
                                                                                      this.get_list_user_inheriting_users();
                                                                                                                                                                                                                                                                }
  }
</script>

<style type="text/css">
  .bg {
    background: white;
  }

  .form.p_4 {
    padding: 1rem;
  }

  .form .el-input {
    width: initial;
  }

  .mt {
    margin-top: 1rem;
  }

  .text_center {
    text-align: center;
  }

  .float-right {
    float: right;
  }


  .modal_wrap{
    width: 100vw;
    height: 100vh;
    position: fixed;
    top: 0;
    left: 0;
    background: rgba(0,0,0,0.5);
    z-index: 9999999999;
  }
  .modal_wrap .modal_box{
    width: 400px;
    height: auto;
    background: url("../../assets/modal_bg.jpg") no-repeat center;
    background-size: cover;
    position: absolute;
    top: 50%;
    left: 50%;
    margin-left: -200px;
    margin-top: -100px;
    border-radius: 10px;
    padding: 10px;
  }
  .modal_wrap .modal_box .modal_box_close{
    font-size: 20px;
    position: absolute;
    top: 10px;
    right: 10px;
    cursor: pointer;
  }
  .modal_wrap .modal_box .modal_box_title{
    text-align: center;
    font-size: 18px;
    margin: 16px auto;
    color: #fff;
    border-bottom: 1px solid rgba(117, 116, 116,0.5);
    padding-bottom: 16px;
    width: 356px;
  }
  .modal_wrap .modal_box .modal_box_text{
    text-align: center;
    font-size: 14px;
    color: #fff;
    margin: 5px auto;
    width: 90%;
  }
  .modal_wrap .modal_box .btn_box{
    display: flex;
    flex-direction: row;
    justify-content: center;
    margin-top: 42px;
  }
  .modal_wrap .modal_box .btn_box span{
    display: inline-block;
    width: 80px;
    height: 30px;
    line-height: 30px;
    text-align: center;
    border: 1px solid #ccc;
    font-size: 14px;
    cursor: pointer;
    color: #fff;
  }
  .modal_wrap .modal_box .btn_box span:nth-child(2){
    background: #409EFF;
    color: #fff;
    border-color: #409EFF;
    margin-left: 15px;
  }
</style>
