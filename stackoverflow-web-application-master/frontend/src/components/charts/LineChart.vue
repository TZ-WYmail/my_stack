<template>
  <div class="my-chart">
    <v-chart ref="line_chart" class="chart" :option="chart_option" autoresize />
  </div>
</template>

<script>

import {use} from "echarts/core";
import {CanvasRenderer} from "echarts/renderers";
import {LineChart} from "echarts/charts";
import {DataZoomComponent, GridComponent, TitleComponent, ToolboxComponent, TooltipComponent} from "echarts/components";
import VChart from "vue-echarts";


export default {
  name: "LineChart",
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
      LineChart,
      TitleComponent,
      TooltipComponent,
      ToolboxComponent,
      GridComponent,
      DataZoomComponent
    ]);
  },
  data() {
    return {
      chart_option: {
        tooltip: {
          trigger: 'axis',
        },
        title: {
          left: 'center',
          text: this.$props.title,
          textStyle: {
            color: 'WHITE',
          },
        },
        toolbox: {
          feature: {
            saveAsImage: {}
          }
        },
        xAxis: {
          type: 'category',
          boundaryGap: false
        },
        yAxis: {
          type: 'value',
          boundaryGap: [0, '100%']
        },
        dataZoom: [
          {
            type: 'inside',
            start: 0,
            end: 100
          },
          {
            start: 0,
            end: 20
          }
        ],
        series: [
          {
            name: '',
            type: 'line',
            smooth: false,
            symbol: 'none',
            areaStyle: {},
            data: []
          }
        ]
      }
    }
  },
}
</script>

<style scoped>

</style>