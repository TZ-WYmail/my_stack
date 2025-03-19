<template>
  <StatisticsContainer>
    <template v-slot:heading>
      <p>User Participation Distribution</p>
    </template>
    <template v-slot:introduction>
      <p>This diagram show the distribution of user participation in thread.</p>
    </template>
    <template v-slot:parameters_input>
      <div class="parameters">
        <DateInputComponent ref="date_input"/>
        <el-button type="primary" style="width: 150px" :loading="is_loading" @click="onQuery">Query
        </el-button>
      </div>
    </template>
    <template v-slot:chart>
      <LineChart v-if="display_chart" ref="chart" title="Thread Participation"/>
    </template>
  </StatisticsContainer>
</template>

<script>
import LineChart from "@/components/charts/LineChart.vue";
import StatisticsContainer from "@/components/StatisticsContainer.vue";
import DateInputComponent from "@/components/parameters/DateInputComponent.vue";
import dayjs from "dayjs";
import axios from "axios";

export default {
  name: "UserParticipation",
  components: {LineChart, StatisticsContainer, DateInputComponent},
  data() {
    return {
      is_loading: false,
      data_values: [],
      display_chart: false,
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
      axios.get("/api/user/participation-distribution", {
        params: {
          from: date_from_string,
          end: date_end_string,
        }
      }).then((response) => {
        this.data_values = response.data
        this.display_chart = true
        this.$nextTick(() => {
          this.$refs.chart.$data.chart_option.series[0].data = this.data_values
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