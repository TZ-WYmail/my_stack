<template>
  <div class="my-chart">
    <v-chart ref="pie_chart" class="chart" :option="chart_option" autoresize />
  </div>
</template>

<script>
import {use} from "echarts/core";
import {CanvasRenderer} from "echarts/renderers";
import {PieChart} from "echarts/charts";
import {LegendComponent, TitleComponent, ToolboxComponent, TooltipComponent} from "echarts/components";
import VChart from 'vue-echarts';

export default {
  name: "PieChart",
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
      PieChart,
      TitleComponent,
      TooltipComponent,
      ToolboxComponent,
      LegendComponent
    ]);
  },
  data() {
    return {
      chart_option: {
        title: {
          text: this.$props.title,
          left: 'center',
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
          trigger: 'item',
          formatter: '<h4>{b}</h4>{c} ({d}%)',
        },
        legend: {
          orient: 'vertical',
          left: 'left',
          data: [],
          textStyle: {
            color: 'WHITE',
          },
        },
        series: [
          {
            name: '',
            type: 'pie',
            radius: '55%',
            center: ['50%', '65%'],
            data: [],
            label: {
              show: true,
              fontSize: 16,
              textStyle: {
                color: 'WHITE',
              },
              edgeDistance: '25%',
              bleedMargin: 5,
            },
            emphasis: {
              itemStyle: {
                shadowBlur: 10,
                shadowOffsetX: 0,
                shadowColor: 'rgba(0, 0, 0, 0.5)',
              },
              label: {
                show: true,
                fontSize: 20,
                fontWeight: 'bold',
              }
            },
          },
        ],
      },
    };
  },

}



</script>

<style scoped>
</style>