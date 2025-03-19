<template>
  <StatisticsContainer>
    <template v-slot:heading>
      <p>Resolution Time Distribution</p>
    </template>
    <template v-slot:introduction>
      <p>This diagram show the distribution of resolution time of distribution.</p>
    </template>
    <template v-slot:parameters_input>
      <div class="parameters">
        <DateInputComponent ref="date_input"/>
        <el-button type="primary" style="width: 150px" :loading="is_loading" @click="onQuery">Query
        </el-button>
      </div>
    </template>
    <template v-slot:chart>
      <LineChart v-if="display_chart" ref="chart" title="Resolution Time Distribution"/>
    </template>
  </StatisticsContainer>
</template>

<script>
import StatisticsContainer from "@/components/StatisticsContainer.vue";
import DateInputComponent from "@/components/parameters/DateInputComponent.vue";
import {ref} from "vue";
import LineChart from "@/components/charts/LineChart.vue";
import dayjs from "dayjs";
import axios from "axios";

export default {
  name: "ResolutionTimeDistribution",
  components: {LineChart, StatisticsContainer, DateInputComponent},
  data() {
    return {
      is_loading: false,
      data_values: [],
      display_chart: ref(false),
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
      this.is_loading = true
      axios.get("/api/question/with-accepted-answer/resolution-distribution", {
        params: {
          from: date_from_string,
          end: date_end_string,
        }
      }).then((response) => {
        this.data_values = response.data
        let accumulate_data = [];
        for (let i = 0; i < this.data_values.length; i++) {
          if (i === 0) {
            accumulate_data.push([this.data_values[i][0] / 1000, this.data_values[i][1]])
          } else {
            accumulate_data.push([this.data_values[i][0] / 1000, this.data_values[i][1] + accumulate_data[i - 1][1]])
          }
        }
        this.display_chart = true
        this.$nextTick(() => {
          this.$refs.chart.$data.chart_option.series[0].data = accumulate_data
        })
      }).catch((error) => {
        this.$message({
          message: 'Failed to query data.',
          type: 'error'
        })
      }).finally(() => {
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