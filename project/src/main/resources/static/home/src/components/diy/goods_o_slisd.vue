<template>
	<div class="list_goods list list-x goods_al">
        <div class="goods__slisd_boxs">
            <h4 class="titles">今日热榜</h4>
        <el-carousel :interval="4000" type="card" height="200px">
                <el-carousel-item  v-for="(o, i) in list" :key="i">
                    <a class="router-link-exact-active router-link-active" @click="goodsClickFn('/goods/details?' + vm.goods_id + '=' + o[vm.goods_id], o)">
                        <div class="media">
                            <div class="icon h-100">
                                <img :src="$fullUrl(o[vm.img])" style="width:100%;height:100%" v-default-img="'../../../public/img/default.png'" />
                            </div>
                            <a class="router-link-exact-active router-link-active">
                                <div class="title">{{ o[vm.title] }}</div>
                            </a>
                            <div class="bottom">
                                <span class="price">￥{{ o[vm.price] }}</span>
                                <span class="price_ago">￥{{ o[vm.price_ago] }}</span>
                                
                            </div>
                        </div>
			        </a>
                </el-carousel-item>
            </el-carousel> 
        </div>
        
		</div>
        
	
</template>

<script>
	export default {
		props: {
			url: {
				type: String,
				default: "/pages/goods/details?goods_id=",
			},
			list: {
				type: Array,
				default: function() {
					return [];
				},
			},
			vm: {
				type: Object,
				default: function() {
					return {
						goods_id: "goods_id",
						img: "img",
						title: "title",
						price: "price",
						price_ago: "price_ago",
						source_table:"source_table"
					};
				},
			},
		},
		data() {
			return {
				user_id: this.$store.state.user.user_id
			};
		},
		methods: {
			/**
			 * 添加购物车
			 */
			add_cart(obj) {
			    var {
			        title,
			        img,
			        price,
			        price_ago,
			        num_buy: num,
			        description,
			        goods_id,
			        type,
			    } = obj;
			    var body = {
			        title,
			        img,
			        price,
			        price_ago,
			        num,
			        price_count: price,
			        description,
			        goods_id,
			        type,
			        user_id: this.$store.state.user.user_id,
			    };
				body.num = 1;

			    this.$get("~/api/cart/get_list?", {
			        goods_id: obj.goods_id,
					user_id: this.$store.state.user.user_id
			    }, (res) => {
			        console.log(res)
			        if (res.result.count) {
			            var {cart_id, num, price, price_count} = res.result.list[0];
			            num += 1;
			            price_count += price;
			            this.$post(
			                `~/api/cart/set?cart_id=${cart_id}`,
			                {
			                    num,
			                    price,
			                    price_count,
			                },
			                (res) => {
			                    this.$toast("已加入购物车", "成功");
			                    console.log("更改值")
			                }
			            );
			        } else {
			            this.$post("~/api/cart/add?", body, (res) => {
			                this.$toast("已加入购物车", "成功");
			            });
			        }
			    })
			}
			,goodsClickFn(url, o) {
				console.log("qwe")
				this.$post(
				    `~/api/goods/set?goods_id=${o.goods_id}`,
				    {
				        hits: parseInt(o.hits) + 1
				    },
				    (res) => {
			            if (res.result) {
			                this.$router.push(url);
			            } else {
			                this.$toast(res.error.message, "error");
			            }
				    }
				);
			}
		}
	};
</script>

<style scoped>
	.media {
		display: flex;
		flex-direction: column;
		justify-content: space-between;
		flex-basis: 75%;
		min-height: 10rem;
	}

	.goods {
		display: flex;
		width: calc(25% - 1rem);
		margin: 0.5rem;
		flex-direction: column;
		justify-content: space-between;
		background-color: white;
		border-radius: 0.5rem;
	}

	.goods:hover {
		border: 0.2rem solid #909399;
		box-shadow: 0 0.1rem 0.3rem rgba(0, 0, 0, 0.15);
	}

	.goods:hover img {
		filter: blur(1px);
	}

    
	.price {
		font-size: 1rem;
		margin-right: 3px;
	}

	.price_ago {
		text-decoration: line-through;
		font-size: 0.5rem;
		color: #999;

	}

    .goods__slisd_boxs{
        width: 350px;
        border: none;
    background: #fafafa;
    border-radius: 11px;
    }
	.title {
		word-break: break-all;
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
		font-weight: 700;
		padding: .25rem .5rem;
	}
	.bottom {
		padding: 0 .5rem .5rem 0.5rem;
	}
	.icon_cart {
		color: #FF5722;
		float: right;
	}

	@media (max-width: 992px) {

		.goods {
			width: calc(33% - 1rem);
			;
		}
      

	}

	@media (max-width: 768px) {

		.goods {
			width: calc(50% - 1rem);
			;
		}
        .goods_al{
            position: relative !important;
            left: 0 !important;
            display: flex;
            justify-content: space-around;
        }

	}
</style>
