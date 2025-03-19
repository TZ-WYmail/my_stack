<template>
  <div class="my-chart">
    <v-chart ref="bar_chart" class="chart" :option="chart_option" autoresize />
  </div>
</template>

<script>


import VChart from "vue-echarts";
import {use} from "echarts/core";
import {CanvasRenderer} from "echarts/renderers";
import {BarChart} from "echarts/charts";
import {GridComponent, TitleComponent, ToolboxComponent, TooltipComponent} from "echarts/components";

export default {
  name: "VerticalBarChart",
  components: {
    VChart
  },
  props: {
    title: {
      type: String,
      default: ''
    },
  },
  setup() {
    use([
      CanvasRenderer,
      BarChart,
      TitleComponent,
      TooltipComponent,
      GridComponent,
      ToolboxComponent
    ]);
  },
  data() {
    return {
      chart_option: {
        title: {
          text: this.$props.title,
          left: 'left',
          textStyle: {
            color: 'WHITE',
          },
        },
        toolbox: {
          feature: {
            saveAsImage: {}
          }
        },
        tooltip: {
          trigger: 'axis',
          axisPointer: {
            type: 'shadow',
          },
        },
        grid: {
          containLabel: true,
          left: 'left'
        },
        yAxis: {
          type: 'category',
          data: [],
          axisLabel: {
            color: 'WHITE',
          },
        },
        xAxis: {
          type: 'value',
        },
        series: [
          {
            type: 'bar',
            data: [],
            barMaxHeight: 100,
          },
        ],
      },
    }
  },
}
</script>

<style scoped>

</style>