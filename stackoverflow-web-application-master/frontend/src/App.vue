<script>
import HeaderComponent from "@/components/HeaderComponent.vue";
import IntroductionComponent from "@/components/IntroductionComponent.vue";
import ContentComponent from "@/components/ContentComponent.vue";


export default {
    name: 'App',
    components: {ContentComponent, IntroductionComponent, HeaderComponent},
    data() {
      return {
        headerMargin: {
          marginTop: '0px'
        }
      }
    },
    mounted() {
      this.headerMargin.marginTop = this.$refs.header.$data.height + 'px'
    }
  }
</script>

<template>
  <div>
    <HeaderComponent ref="header"/>
  </div>
  <div :style="headerMargin">
    <IntroductionComponent/>
  </div>
  <div>
    <ContentComponent/>
  </div>
</template>