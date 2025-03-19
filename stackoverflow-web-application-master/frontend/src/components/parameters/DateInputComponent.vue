<template>
  <div class="date-input-main">
    <el-date-picker
        v-model="from"
        type="date"
        placeholder="From"
        size="default"
        style="width: 150px;"
    />
    <el-date-picker
        v-model="end"
        type="date"
        placeholder="End"
        size="default"
        style="width: 150px;"
    />
  </div>
</template>

<script>
import {ref} from 'vue'


export default {
  name: "DateInputComponent",
  data() {
    return {
      from: ref(),
      end: ref(),
    }
  }
}
</script>


<style scoped>

.date-input-main {
  display: flex;
  flex-direction: row;
  gap: 20px;
}

</style>