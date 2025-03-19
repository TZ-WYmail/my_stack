<template>
  <StatisticsContainer>
    <template v-slot:heading>
      <p>Active User</p>
    </template>
    <template v-slot:introduction>
      <p>This diagram show the top 10 active users.</p>
    </template>
    <template v-slot:parameters_input>
      <div class="parameters">
        <DateInputComponent ref="date_input"/>
        <el-button type="primary" style="width: 150px" :loading="is_loading" @click="onQuery">Query
        </el-button>
      </div>
    </template>
    <template v-slot:chart>
      <VerticalBarChart v-if="display_chart" ref="chart" title="Active Users"/>
    </template>
  </StatisticsContainer>
</template>

<script>
import {ref} from "vue";
import StatisticsContainer from "@/components/StatisticsContainer.vue";
import DateInputComponent from "@/components/parameters/DateInputComponent.vue";
import VerticalBarChart from "@/components/charts/VerticalBarChart.vue";
import dayjs from "dayjs";
import axios from "axios";

export default {
  name: "ActiveUser",
  components: {StatisticsContainer, DateInputComponent, VerticalBarChart},
  data() {
    return {
      is_loading: false,
      category_data: [],
      data_value: [],
      display_chart: ref(false)
    }
  },
  methods: {
    onQuery() {
      if (typeof this.$refs.date_input.$data.from === 'undefined'
          || typeof this.$refs.date_input.$data.end === 'undefined') {
        this.$message({
          message: 'Please select a date range.',
          type: 'warning'
        })
        return
      }
      this.display_chart = false
      this.is_loading = true
      let date_from_string = dayjs(this.$refs.date_input.$data.from.toString()).format('YYYY-MM-DD') + 'T00:00:00'
      let date_end_string = dayjs(this.$refs.date_input.$data.end.toString()).format('YYYY-MM-DD') + 'T00:00:00'
      axios.get("/api/user/activity", {
          params: {
            from: date_from_string,
            end: date_end_string
          }
        }).then((response) => {
          console.log(response.data)
          for (let i = 0; i < 10; ++i) {
            this.category_data[9 - i] = response.data[i].user.displayName
            if (this.category_data[9 - i] === 'does_not_exist') {
              this.category_data[9 - i] = '★ Anonymous'
            }
            this.data_value[9 - i] = response.data[i].activity
          }
          this.display_chart = true
          this.$nextTick(() => {
            this.$refs.chart.chart_option.yAxis.data = this.category_data
            this.$refs.chart.chart_option.series[0].data = this.data_value
          })
          this.is_loading = false
        }).catch((error) => {
          this.$message({
            message: error.response.data.message,
            type: 'error'
          })
          this.is_loading = false
        })

    }
  }
}
</script>

<style scoped>
.parameters {
  display: flex;
  flex-direction: row;
  gap: 20px;
}
</style>