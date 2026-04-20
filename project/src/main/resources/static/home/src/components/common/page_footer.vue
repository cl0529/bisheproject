<template>
  <footer class="page_footer">
    <div class="wrap">
      <div class="container">
        <div class="row">
          <div class="col">
            <div class="card_copyright">
                   <div class="is1">
                <span class="as1" ></span>
                <span class="as2"></span>
                <span class="as3"></span>
                <span class="as4"></span>
              </div>
              <div class="is2">
                <span class="bs1"></span>
                <span class="bs2"></span>
                <span class="bs3"></span>
                <span class="bs4"></span>
                </div>
              <div class="is3">
                <span class="cs1"></span>
                <span class="cs2"></span>
                <span class="cs3"></span>
                <span class="cs4"></span>
              </div>
              <div class="is4">
                <span class="ds1"></span>
                <span class="ds2"></span>
                <span class="ds3"></span>
                <span class="ds4"></span>
              </div>
              
            </div>
          </div>
        </div>
      </div>
    </div>
  </footer>
</template>

<script>
import mixin from "@/mixins/component.js";
export default {
  mixins: [mixin],
  components: {
      },
  data() {
    return {
          };
  },
  methods: {
      },
  created() {
      },
};
</script>

<style scoped="scoped">

.page_footer .wrap {
    background-color: #e2e2e2;
  color: rgb(167, 167, 167);
}
.card_copyright {
	padding: 30px 10px;
  text-align: center;
}
</style>
