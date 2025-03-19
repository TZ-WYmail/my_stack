<template>
  <StatisticsContainer>
    <template v-slot:heading>
      <p>Average / Maximum Answers</p>
    </template>
    <template v-slot:introduction>
      <p>This diagram show the average and maximum numbers of answers of questions.</p>
    </template>
    <template v-slot:parameters_input>
      <div class="parameters">
        <DateInputComponent ref="date_input"/>
        <el-button type="primary" style="width: 150px" :loading="is_loading" @click="onQuery">Query
        </el-button>
      </div>
    </template>
    <template v-slot:chart>
      <VerticalBarChart v-if="display_chart" ref="chart" title="Average / Maximum Answers"/>
    </template>
  </StatisticsContainer>
</template>

<script>
import VerticalBarChart from "@/components/charts/VerticalBarChart.vue";
import {ref} from "vue";
import axios from "axios";
import dayjs from "dayjs";
import DateInputComponent from "@/components/parameters/DateInputComponent.vue";
import StatisticsContainer from "@/components/StatisticsContainer.vue";

export default {
  name: "AverageMaxAnswer",
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
      this.display_chart = false;
      let date_from_string = dayjs(this.$refs.date_input.$data.from.toString()).format('YYYY-MM-DD') + 'T00:00:00'
      let date_end_string = dayjs(this.$refs.date_input.$data.end.toString()).format('YYYY-MM-DD') + 'T00:00:00'
      this.is_loading = true;
      axios.get("api/question/answer/average", {
        params: {
          from: date_from_string,
          end: date_end_string,
        }
      }).then(response => {
        axios.get("/api/question/answer/max", {
          params: {
            from: date_from_string,
            end: date_end_string
          }
        }).then(response2 => {
          this.category_data = ["Average", "Max"]
          this.data_value = [response.data, response2.data]
          this.display_chart = true
          this.$nextTick(() => {
            this.$refs.chart.$data.chart_option.series[0].data = this.data_value
            this.$refs.chart.$data.chart_option.yAxis.data = this.category_data
          })
        }).catch(error => {
          this.$message({
            message: 'Failed to query data.',
            type: 'error'
          })
        }).finally(() => {
          this.is_loading = false
        })

      }).catch(error => {
        this.$message({
          message: 'Failed to query data.',
          type: 'error'
        })
        this.is_loading = false
      })
    },
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