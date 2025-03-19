<template>
  <StatisticsContainer>
    <template v-slot:heading>
      <p>Question With Answer Better Than Accepted</p>
    </template>
    <template v-slot:introduction>
      <p>This diagram show the distribution of questions with not accepted answers better than accepted answer.</p>
    </template>
    <template v-slot:parameters_input>
      <div class="parameters">
        <DateInputComponent ref="date_input"/>
        <el-button type="primary" style="width: 150px" :loading="is_loading" @click="onQuery">Query
        </el-button>
      </div>
    </template>
    <template v-slot:chart>
      <PieChart v-if="display_chart" ref="chart" title="Question With Better Answer"/>
    </template>
  </StatisticsContainer>
</template>

<script>
import PieChart from "@/components/charts/PieChart.vue";
import DateInputComponent from "@/components/parameters/DateInputComponent.vue";
import StatisticsContainer from "@/components/StatisticsContainer.vue";
import {ref} from "vue";
import dayjs from "dayjs";
import axios from "axios";

export default {
  name: "BetterAnswerThanAccepted",
  components: {DateInputComponent, PieChart, StatisticsContainer},
  data() {
    return {
      is_loading: false,
      legend_data: [],
      series_data: [],
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
      this.is_loading = true
      this.legend_data = ["No Better Answer", "With Better Answer"]
      let date_from_string = dayjs(this.$refs.date_input.$data.from.toString()).format('YYYY-MM-DD') + 'T00:00:00'
      let date_end_string = dayjs(this.$refs.date_input.$data.end.toString()).format('YYYY-MM-DD') + 'T00:00:00'
      axios.get("/api/question/with-accepted-answer/better-answer/total", {
        params: {
          from: date_from_string,
          end: date_end_string,
        }
      }).then(response => {
        this.series_data[1] = {value: response.data, name: 'With Better Answer'}
        axios.get("/api/question/with-accepted-answer/total", {
          params: {
            from: date_from_string,
            end: date_end_string,
          }
        }).then(response => {
          this.series_data[0] = {value: response.data - this.series_data[1].value, name: 'No Better Answer'}
          this.display_chart = true;
          this.$nextTick(() => {
            this.$refs.chart.$data.chart_option.series[0].data = this.series_data
            this.$refs.chart.$data.chart_option.legend.data = this.legend_data
          })
        }).catch(error => {
          this.$message({
            message: error.response.data.message,
            type: 'error'
          })
        }).finally(() => {
          this.is_loading = false
        })
      }).catch(error => {
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