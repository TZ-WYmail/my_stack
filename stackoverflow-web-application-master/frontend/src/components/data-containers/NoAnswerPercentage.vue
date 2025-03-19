<template>
  <StatisticsContainer>
    <template v-slot:heading>
      <p>Question With No Answers</p>
    </template>
    <template v-slot:introduction>
      <p>This diagram show the distribution of questions with no answer.</p>
    </template>
    <template v-slot:parameters_input>
      <div class="parameters">
        <DateInputComponent ref="date_input"/>
        <el-button type="primary" style="width: 150px" :loading="is_loading" @click="onQuery">Query
        </el-button>
      </div>
    </template>
    <template v-slot:chart>
      <PieChart v-if="display_chart" ref="chart" title="Question With No Answers"/>
    </template>
  </StatisticsContainer>
</template>

<script>
import StatisticsContainer from "@/components/StatisticsContainer.vue";
import PieChart from "@/components/charts/PieChart.vue";
import DateInputComponent from "@/components/parameters/DateInputComponent.vue";
import {ref} from "vue";
import axios from "axios";
import dayjs from 'dayjs';

export default {
  name: "NoAnswerPercentage",
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
      this.legend_data = ["No Answer", "With Answer"]
      let date_from_string = dayjs(this.$refs.date_input.$data.from.toString()).format('YYYY-MM-DD') + 'T00:00:00'
      let date_end_string = dayjs(this.$refs.date_input.$data.end.toString()).format('YYYY-MM-DD') + 'T00:00:00'
      axios.get("/api/question/no-answer/total", {
        params: {
          from: date_from_string,
          end: date_end_string,
        }
      }).then(response => {

        this.series_data[0] = {value: response.data, name: "No Answer"}
        axios.get("/api/question/with-answer/total", {
          params: {
            from: date_from_string,
            end: date_end_string
          }
        })
        .then(response2 => {
          this.series_data[1] = {value: response2.data, name: "With Answer"}
          this.display_chart = true;
          this.$nextTick(() => {
            this.$refs.chart.$data.chart_option.series[0].data = this.series_data
            this.$refs.chart.$data.chart_option.legend.data = this.legend_data
          })

        })
        .catch(error => {
          this.$message({
            message: 'Failed to query data.',
            type: 'error'
          })
        }).finally(() => {
          this.is_loading = false;
        })
      })
      .catch(error => {
        this.$message({
          message: 'Failed to query data.',
          type: 'error'
        })
        this.is_loading = false;
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