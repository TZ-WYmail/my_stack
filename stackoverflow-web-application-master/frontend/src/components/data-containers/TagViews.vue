<template>
  <StatisticsContainer>
    <template v-slot:heading>
      <p>Tags Views</p>
    </template>
    <template v-slot:introduction>
      <p>This diagram show the top 10 tags or tag combinations with most views.</p>
    </template>
    <template v-slot:parameters_input>
      <div class="parameters">
        <DateInputComponent ref="date_input"/>
        <NumberInputComponent ref="number_input" min_value=1 max_value=4 />
        <el-button type="primary" style="width: 150px" :loading="is_loading" @click="onQuery">Query
        </el-button>
      </div>
    </template>
    <template v-slot:chart>
      <VerticalBarChart v-if="display_chart" ref="chart" title="Tags Views"/>
    </template>
  </StatisticsContainer>
</template>

<script>
import StatisticsContainer from "@/components/StatisticsContainer.vue";
import DateInputComponent from "@/components/parameters/DateInputComponent.vue";
import VerticalBarChart from "@/components/charts/VerticalBarChart.vue";
import {ref} from "vue";
import NumberInputComponent from "@/components/parameters/NumberInputComponent.vue";
import dayjs from "dayjs";
import axios from "axios";

export default {
  name: "TagViews",
  components: {NumberInputComponent, StatisticsContainer, DateInputComponent, VerticalBarChart},
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
      if (this.$refs.number_input.$data.num === 1) {
        axios.get("/api/tag/single-tag/view", {
          params: {
            from: date_from_string,
            end: date_end_string,
          }
        }).then(response => {
          for (let i = 0; i < 10; ++i) {
            this.category_data[9 - i] = response.data[i].tag
            this.data_value[9 - i] = response.data[i].count
          }
          this.display_chart = true
          this.$nextTick(() => {
            this.$refs.chart.chart_option.series[0].data = this.data_value
            this.$refs.chart.chart_option.yAxis.data = this.category_data
          })
          this.is_loading = false
        }).catch(error => {
          this.$message({
            message: 'Failed to query data.',
            type: 'error'
          })
          this.is_loading = false
        })
      } else {
        axios.get("/api/tag/group-tag/view", {
          params: {
            from: date_from_string,
            end: date_end_string,
            size: this.$refs.number_input.$data.num,
          }
        }).then(response => {
          for (let i = 0; i < 10; ++i) {
            this.category_data[9 - i] = response.data[i].tags
            this.data_value[9 - i] = response.data[i].count
          }
          this.display_chart = true
          this.$nextTick(() => {
            this.$refs.chart.chart_option.series[0].data = this.data_value
            this.$refs.chart.chart_option.yAxis.data = this.category_data
          })
          this.is_loading = false
        }).catch(error => {
          this.$message({
            message: 'Failed to query data.',
            type: 'error'
          })
          this.is_loading = false
        })
      }
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