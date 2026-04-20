<template>
    <div class="page_goods goods_list_s" id="goods_list">
        <div class="warp" v-if="$check_action('/goods/list', 'get')">
            <div class="container">
                <div class="left_box">
                    <span></span>
                    <span></span>
                    <span></span>
                </div>
                <div class="row page_goods_list_wrap">
                    <div class="col-12 col-lg-10 col-md-12 page_goods_list_base">
                        <div class="card_goods_list">
                            <div class="good_list_title">
                                <span class="title">商品列表</span>
                               
                            </div>
                            <div class="leis_box">
                                <div class="iudis_box">
                                    <!-- 搜索栏 -->
                                <div class="search_box">
                                    <b-form-input
                                            size="sm"
                                            class="mr-sm-2"
                                            placeholder="局部搜索"
                                            v-model.trim="search_val"
                                            @keyup.enter="submit_search"
                                    />
                                    <b-button size="sm" @click="submit_search"
                                    >
                                        <b-icon icon="search"/>
                                    </b-button>
                                </div>
                                    <div class="filter_block">
                                        <!-- 筛选 -->
                                        <div class="bar_filter">
                                            <b-dropdown :text="title" variant="outline-dark" left>
                                                <b-dropdown-item
                                                        v-for="(o, i) in list_goods_type"
                                                        :key="i"
                                                        @click="filter_goods(o)"
                                                >
                                                    {{ o }}
                                                </b-dropdown-item>
                                            </b-dropdown>
                                        </div>
                                        <!-- /筛选 -->
                                        <!-- 排序 -->
                                        <div class="bar_sort"
                                                :class="{ active: query.orderby == '`hits` desc' }"
                                        >
                                            <b-dropdown text="排序" variant="outline-dark" left>
                                                <b-dropdown-item
                                                        v-for="(o, i) in sort_list"
                                                        :key="i"
                                                        @click="sort_goods(o)"
                                                >
                                                    {{ o.name }}
                                                </b-dropdown-item>
                                            </b-dropdown>
                                        </div>
                                        <!--/排序 -->
                                    </div>
                            
                             
                                </div>
                                    <!-- 商品列表 -->
                                <div class="lis_i_box">
                                    <list_goods :list="list"/>
                                    <godds_o_i :list="list_hot"/>
                                </div>
                            </div>
                            
                           
                        </div>
                        <!-- 分页器 -->
                        <div class="col overflow-auto flex_cc">
                          <b-pagination
                              v-model="query.page"
                              :total-rows="count"
                              :per-page="query.size"
                              @change="goToPage"
                          />
                        </div>
                    </div>
                    <div class="col-12 col-lg-2 page_goods_list_hot">
                        <div class="card_list_hot">
                            <!-- 商品热门列表 -->
                            <div class="hot_title">
                                商品热门列表
                            </div>
                            <list_goods_hot :list="list_hot"/>
                        </div>
                    </div>
                </div>
             
            </div>
        </div>
    </div>
</template>

<script>
    import mixin from "@/mixins/page.js";
    import list_goods from "@/components/diy/list_goods.vue";
    import list_goods_hot from "@/components/diy/list_goods_hot.vue";
    import godds_o_i from "@/components/diy/goods_o_slisd.vue"

    export default {
        mixins: [mixin],
        components: {
            list_goods,
            list_goods_hot,
            godds_o_i
        },
        data() {
            return {
                url_get_list: "~/api/goods/get_list?",
                query: {
                    page: 1,
                    size: 12,
                },
                list_hot: [],
                list_goods_type: [],
                title: "",
                sort_list: [
                    {
                        name: "价格从高到低",
                        value: "price desc",
                    },
                    {
                        name: "价格从低到高",
                        value: "price asc",
                    },
                    {
                        name: "创建时间从高到低",
                        value: "create_time desc",
                    },
                    {
                        name: "创建时间从低到高",
                        value: "create_time asc",
                    },
                    {
                        name: "销量从高到低",
                        value: "sales desc",
                    },
                    {
                        name: "销量从低到高",
                        value: "sales asc",
                    }
                ],
                search_val: "",
            };
        },
        methods: {
            /**
             *  获取热门商品
             * */
            get_hot_goods() {
            	let url = "~/api/goods/get_list?";
            	let param = {
            		page: 1,
        			size: 5,
        			orderby: "hits"
            	};
                	                	                	                	                	                	                	                        this.$get(
                    url,
                    param,
                    (res) => {
						if (res.result) {
							this.list_hot = res.result.list.slice(0,5);
						}
					}
                );
            },
            /**
             *  搜索
             * */
            submit_search() {
                this.$router.push({
                    path: "/search/details",
                    query: {
                        word: this.search_val,
                        search_key: "title",
                        search_type: "goods",
                        search_title: "商品",
                    },
                });
            },
            /**
             *添加商品类型过滤
             */
            get_goods_type() {
                this.$get(
                    "~/api/goods_type/get_list",
                    {},
                    (res) => {
                        if (res.result) {
                            this.list_goods_type.push("全部");
                            res.result.list.map((o) => this.list_goods_type.push(o.name));
                        }
                    }
                );
            },
            /**
             * 商品筛选选择
             */
            filter_goods(o) {
                if (o == "全部") {
                    this.query.type = 0;
                } else {
                    this.query.type = o;
                }
                this.search();
            },
            /**
             * 排序
             */
            sort_goods(o) {
                console.log("asd", o);
                this.query.orderby = o.value;
                this.search();
            },
            goToPage(v){
              this.query.page = v;
              this.goToNew(v)
            }
        },
        mounted() {
            this.title = "筛选";
            this.get_hot_goods();
            this.get_goods_type();
        },
    };
</script>

<style scoped>


.goods_al{
        position: absolute;
    left: -24rem;
    top: 21px;
    
    }

    .goods__slisd_boxs{
        width: 350px;
        border: none;
    background: #fafafa;
    border-radius: 11px;
    }

    .el-carousel__item h3 {
    color: #475669;
    font-size: 14px;
    opacity: 0.75;
    line-height: 200px;
    margin: 0;
  }
  
  .el-carousel__item:nth-child(2n) {
    background-color: #99a9bf;
  }
  
  .el-carousel__item:nth-child(2n+1) {
    background-color: #d3dce6;
  }


    .flex_cc {
        display: flex;
        justify-content: center;
        align-items: center;
    }

    .card_goods_list {
        margin: 0.25rem 0.25rem 2rem 0.25rem;
        padding-top: 1rem;
    }

    .card_list_hot {
        background-color: var(--color_primary);
        margin: 1.25rem 0.5rem 0.5rem 0.5rem;
        border-radius: 0.5rem;
    }

    .filter_block {
        margin: 1rem 0.5rem 0.5rem 0.5rem;
        display: flex;
        flex-direction: row;
        justify-content: start;
    }

    .bar_filter {
        display: flex;
        align-items: center;
    }

    li {
        list-style: none;
    }

    .bar_sort {
        margin-left: 5px;
    }

    .search_box {
        width: 12rem;
        display: flex;
        height: 4rem;
        justify-content: center;
        align-items: center;
        padding-right: 1rem;
    }

    .good_list_title {
        margin: 0 0.5rem;
        background-color: #d2d2d2;
        color: white;
        display: flex;
        height: 4rem;
        align-items: center;
        justify-content: space-between;
        border-radius: 0.5rem;
    }

    .title {
        font-size: 1.5rem;
        font-weight: bold;
        padding-left: 1rem;
    }

    .hot_title {
        height: 4rem;
        color: white;
        font-size: 1.2rem;
        font-weight: bold;
        align-items: center;
        justify-content: center;
        display: flex;
        margin-bottom: 1rem;
    }
</style>

