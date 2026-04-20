<template>
	<div class="list_goods list list-x goods_s">

            <div class="carousel">
                <div class="slider" ref="slider">
                <div  v-for="(o, i) in  list" :key="i" class="slide" >
                    
                    <router-link :to="'/goods/details?' + vm.goods_id + '=' + o[vm.goods_id]" class="lis_cont">
                          
                            <img :src="$fullUrl(o[vm.img])" style="width:100%;height:100%" v-default-img="'../../../public/img/default.png'" />
							<div class="title">{{ o[vm.title] }}</div>
                    <div class="doc">
                        <span class="price">￥{{ o[vm.price] }}</span>
                                <span class="price_ago">￥{{ o[vm.price_ago] }}</span>
                                <b-icon class="icon_cart" icon="cart"  v-if="!o[vm.source_table]||$check_cart_page('/'+o[vm.source_table]+'/list')" @click="add_cart(o)" />
                    </div>
                </router-link>
                </div>
                </div>
                <button class="prev" @click="prevSlide">&lt;</button>
                <button class="next" @click="nextSlide">&gt;</button>
            </div>
			<div class="paginations" >
				<button v-for="page in pagarr" :key="page" @click="handleButtonClick(page)" :class="{ pag_btn:page, active: page === activePage }">{{ page }}</button>
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
				user_id: this.$store.state.user.user_id,
                translateX: 0,
                currentIndex: 0,	
                timer: null,
				pagarr:[],
				activePage: 1,
				currentPage: 1,
      			itemsPerPage: 3,
				totalItems: 3,
				
            };
		},
        computed: {
		
        },
        mounted() {
			let lang = this.list.length;
		
			// 指定每次循环添加的数量
			let pagarrLength = Math.ceil(lang / 3);

			// 添加页数
			for (let i = 1; i <= pagarrLength; i++) {
				this.pagarr.push(i);
			}
        },
		watch:{
			'list'(val){
				let lang = this.list.length;
		
				// 指定每次循环添加的数量
				let pagarrLength = Math.ceil(lang / 3);

				// 添加页数
				for (let i = 1; i <= pagarrLength; i++) {
					this.pagarr.push(i);
				}
			}
		},
		methods: {
			/**
			 * 添加购物车
			 */
			 handleButtonClick(index){

				this.activePage = index;
				if (index == 1  ) {
					
					this.translateX = -this.currentIndex * 0;

				}
				if (index == 2  ) {

					this.translateX = -99.99;
					
				}

				if(index == 3 ){
					this.translateX = -199.98
				}
				if(index == 4){
					this.translateX = -299.96999999999997
					
				}
				
				this.$nextTick(() => {
							this.$refs.slider.classList.add('slide-transition');
							
							this.$refs.slider.style.transform = `translateX(${this.translateX}%)`;
						});
				},

				prevSlide() {

				if (this.currentIndex === 0) {
					this.currentIndex = this.$props.list.length - 3;
				} else {
					this.currentIndex--;
				}
				this.translateX = -this.currentIndex * 33.33;

				this.$nextTick(() => {
							this.$refs.slider.classList.add('slide-transition');
							if (this.currentIndex ==9) {
								this.$refs.slider.classList.remove('slide-transition');
							}
							this.$refs.slider.style.transform = `translateX(${this.translateX}%)`;
						});

				},
				nextSlide() {

					if (this.currentIndex===8) {
						this.$refs.slider.classList.remove('slide-transition');
					}
					if ( this.currentIndex === this.$props.list.length - 3) {
						
						this.currentIndex = 0
						
					} else {
						this.currentIndex++;
					}

					this.translateX = -this.currentIndex * 33.33;
					
						this.$nextTick(() => {
							
							this.$refs.slider.classList.add('slide-transition');
							if (this.currentIndex ==0) {
								this.$refs.slider.classList.remove('slide-transition');
							}
							this.$refs.slider.style.transform = `translateX(${this.translateX}%)`;
					});

			},
                   
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
			},
		}
    }
	
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
		.slide {
		
		width: calc(33.33% - 1rem);

		}

	}

	@media (max-width: 768px) {

		.goods {
			width: calc(50% - 1rem);
			;
		}
		.slide {
   
			width: calc(33.33% - 1rem);
   
		}

	}

	
.slide-transition {
  transition: transform 0.5s ease;
}
    .carousel {
        position: relative;
        width: 100%;
        height: 300px; /* 设置轮播图的高度 */
        overflow: hidden;
        }

.slider {
  display: flex;

}

.slide {
    flex-shrink: 0;
	width: calc(33.33% - 1rem);
   
}


.slide img{
    width: 300px;
    height: 200px ;
}

.slide-image {
  width: 100%;
  height: 100%;
  object-fit: cover;
}

.prev,
.next {
  position: absolute;
  top: 50%;
  transform: translateY(-50%);
  width: 40px;
  height: 40px;
  font-size: 24px;
  background-color: #ccc;
  border-radius: 50%;
  border: none;
  color: #fff;
  opacity: 0.7;
}

.prev {
  left: 10px;
}

.next {
  right: 10px;
}

.paginations {
    display: flex;
    /* padding-left: 0; */
    /* list-style: none; */
    width: 20%;
    left: 41%;
    justify-content: space-around;
    font-size: 25px;
    border: none;
}

button.active {
  background-color: rgb(221, 127, 4);
  color: white;
}
</style>
