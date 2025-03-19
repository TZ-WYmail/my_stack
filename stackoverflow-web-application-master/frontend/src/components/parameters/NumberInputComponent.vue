<template>
  <el-input-number v-model="num" :min=min_value :max=max_value @change="handleChange" />
</template>

<script>

export default {
  name: "NumberInputComponent",
  props: {
    min_value: {
      type: Number,
      default: 1
    },
    max_value: {
      type: Number,
      default: 5
    },
    handleChange: {
      type: Function,
      default: () => {}
    }
  },
  data() {
    return {
      num: 1,
    }
  }
}
</script>

<style scoped>

</style>