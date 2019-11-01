const YESTERDAY = '20140602'

Highcharts.createElement('link', {
    href: 'https://fonts.googleapis.com/css?family=Unica+One',
    rel: 'stylesheet',
    type: 'text/css'
}, null, document.getElementsByTagName('head')[0]);

Highcharts.theme = {
    colors: ['#2b908f', '#90ee7e', '#f45b5b', '#7798BF', '#aaeeee', '#ff0066',
        '#eeaaee', '#55BF3B', '#DF5353', '#7798BF', '#aaeeee'],
    chart: {
        backgroundColor: {
            linearGradient: { x1: 0, y1: 0, x2: 1, y2: 1 },
            stops: [
                [0, '#2a2a2b'],
                [1, '#3e3e40']
            ]
        },
        style: {
            fontFamily: '\'Unica One\', sans-serif'
        },
        plotBorderColor: '#606063'
    },
    title: {
        style: {
            color: '#E0E0E3',
            textTransform: 'uppercase',
            fontSize: '20px'
        }
    },
    subtitle: {
        style: {
            color: '#E0E0E3',
            textTransform: 'uppercase'
        }
    },
    xAxis: {
        gridLineColor: '#707073',
        labels: {
            style: {
                color: '#E0E0E3'
            }
        },
        lineColor: '#707073',
        minorGridLineColor: '#505053',
        tickColor: '#707073',
        title: {
            style: {
                color: '#A0A0A3'

            }
        }
    },
    yAxis: {
        gridLineColor: '#707073',
        labels: {
            style: {
                color: '#E0E0E3'
            }
        },
        lineColor: '#707073',
        minorGridLineColor: '#505053',
        tickColor: '#707073',
        tickWidth: 1,
        title: {
            style: {
                color: '#A0A0A3'
            }
        }
    },
    tooltip: {
        backgroundColor: 'rgba(0, 0, 0, 0.85)',
        style: {
            color: '#F0F0F0'
        }
    },
    plotOptions: {
        series: {
            dataLabels: {
                color: '#F0F0F3',
                style: {
                    fontSize: '13px'
                }
            },
            marker: {
                lineColor: '#333'
            }
        },
        boxplot: {
            fillColor: '#505053'
        },
        candlestick: {
            lineColor: 'white'
        },
        errorbar: {
            color: 'white'
        }
    },
    legend: {
        backgroundColor: 'rgba(0, 0, 0, 0.5)',
        itemStyle: {
            color: '#E0E0E3'
        },
        itemHoverStyle: {
            color: '#FFF'
        },
        itemHiddenStyle: {
            color: '#606063'
        },
        title: {
            style: {
                color: '#C0C0C0'
            }
        }
    },
    credits: {
        style: {
            color: '#666'
        }
    },
    labels: {
        style: {
            color: '#707073'
        }
    },

    drilldown: {
        activeAxisLabelStyle: {
            color: '#F0F0F3'
        },
        activeDataLabelStyle: {
            color: '#F0F0F3'
        }
    },

    navigation: {
        buttonOptions: {
            symbolStroke: '#DDDDDD',
            theme: {
                fill: '#505053'
            }
        }
    },

    // scroll charts
    rangeSelector: {
        buttonTheme: {
            fill: '#505053',
            stroke: '#000000',
            style: {
                color: '#CCC'
            },
            states: {
                hover: {
                    fill: '#707073',
                    stroke: '#000000',
                    style: {
                        color: 'white'
                    }
                },
                select: {
                    fill: '#000003',
                    stroke: '#000000',
                    style: {
                        color: 'white'
                    }
                }
            }
        },
        inputBoxBorderColor: '#505053',
        inputStyle: {
            backgroundColor: '#333',
            color: 'silver'
        },
        labelStyle: {
            color: 'silver'
        }
    },

    navigator: {
        handles: {
            backgroundColor: '#666',
            borderColor: '#AAA'
        },
        outlineColor: '#CCC',
        maskFill: 'rgba(255,255,255,0.1)',
        series: {
            color: '#7798BF',
            lineColor: '#A6C7ED'
        },
        xAxis: {
            gridLineColor: '#505053'
        }
    },

    scrollbar: {
        barBackgroundColor: '#808083',
        barBorderColor: '#808083',
        buttonArrowColor: '#CCC',
        buttonBackgroundColor: '#606063',
        buttonBorderColor: '#606063',
        rifleColor: '#FFF',
        trackBackgroundColor: '#404043',
        trackBorderColor: '#404043'
    }
};

// Apply the theme
Highcharts.setOptions(Highcharts.theme);

const HBASE_COLUMNS = ["productHumidity", "avginputTemperatureProduct", 
                          "avgTemperatureProcess1", "avgTemperatureProcess2", 
                          "avgWaterTemperatureProcess", "avgWaterFlowProcess", 
                          "avgIntensityFanProcess"];
const HBASE_CF = 'METER'
const COLUMNS = ["humidity", "productTemp", "process1Temp", "process2Temp", "waterTemp", "waterFlow", "fanIntensity"]

function extractValues(items) {
  
  var xyPlots = {"humidity": [],
                 "productTemp": [],
                 "process1Temp": [],
                 "process2Temp": [],
                 "waterTemp": [],
                 "waterFlow": [],
                 "fanIntensity": []}
  for (let row of items) {
    var rowkey = row[0]
    var values = row[1]
    var date = rowkey.split("#")[2] // yyyyMMdd
    var endTime = rowkey.split("#")[4] // HHmm

    var year = Number(date.substring(0, 4));
    var monthIndex = Number(date.substring(4, 6)) - 1;
    var day = Number(date.substring(6, 8));
    var hours = Number(endTime.substring(0, 2));
    var minutes = Number(endTime.substring(2, 4));
    var xVal = new Date(year, monthIndex, day, hours, minutes).getTime();

    // must sort the list by order of time, or else HighCharts doesn't understand
    for (var i=0; i < HBASE_COLUMNS.length; i++) {
      console.log(HBASE_CF + ":" + HBASE_COLUMNS[i])
      xyPlots[COLUMNS[i]].push([xVal, values[HBASE_CF + ":" + HBASE_COLUMNS[i]]])
    }
  }

  for (let key of COLUMNS) {
    xyPlots[key] = xyPlots[key].sort((a, b) => (a[0] < b[0]) ? 1 : -1)
  }
  return xyPlots
}

function disp_charts(data) {

  xyPlots = extractValues(data.items)
  console.log("DATAPOINTS: ", xyPlots)

  Highcharts.chart('humidity', {

    chart: {
        scrollablePlotArea: {
            minWidth: 700
        }
    },

    // data: xyPlots['humidity'],

    title: {
        text: 'Product Humidity Measurements of the Past Week'
    },

    xAxis: {
      type: 'datetime'
    },

    yAxis: [{ // left y axis
        title: {
            text: null
        },
        labels: {
            align: 'left',
            x: 3,
            y: 16,
            format: '{value:.,0f}'
        },
        showFirstLabel: false
    }, { // right y axis
        linkedTo: 0,
        gridLineWidth: 0,
        opposite: true,
        title: {
            text: null
        },
        labels: {
            align: 'right',
            x: -3,
            y: 16,
            format: '{value:.,0f}'
        },
        showFirstLabel: false
    }],

    legend: {
        align: 'left',
        verticalAlign: 'top',
        borderWidth: 0
    },

    tooltip: {
        shared: true,
        crosshairs: true
    },

    plotOptions: {
        series: {
            cursor: 'pointer',
            point: {
                events: {
                    click: function (e) {
                        hs.htmlExpand(null, {
                            pageOrigin: {
                                x: e.pageX || e.clientX,
                                y: e.pageY || e.clientY
                            },
                            headingText: this.series.name,
                            maincontentText: Highcharts.dateFormat('%A, %b %e, %Y', this.x) + ':<br/> ' +
                                this.y + ' sessions',
                            width: 200
                        });
                    }
                }
            },
            marker: {
                lineWidth: 1
            }
        }
    },

    series: [{
        name: 'Product Humidity (%)',
        lineWidth: 4,
        marker: {
            radius: 4
        },
        data: xyPlots['humidity']
    }]
  });

  Highcharts.chart('temperatures', {

    chart: {
        scrollablePlotArea: {
            minWidth: 700
        }
    },

    title: {
        text: 'Average Temperatures of the Past Week'
    },

    xAxis: {
        type: 'datetime'
      },  

    yAxis: [{ // left y axis
        title: {
            text: null
        },
        labels: {
            align: 'left',
            x: 3,
            y: 16,
            format: '{value:.,0f}'
        },
        showFirstLabel: false
    }, { // right y axis
        linkedTo: 0,
        gridLineWidth: 0,
        opposite: true,
        title: {
            text: null
        },
        labels: {
            align: 'right',
            x: -3,
            y: 16,
            format: '{value:.,0f}'
        },
        showFirstLabel: false
    }],

    legend: {
        align: 'left',
        verticalAlign: 'top',
        borderWidth: 0
    },

    tooltip: {
        shared: true,
        crosshairs: true
    },

    plotOptions: {
        series: {
            cursor: 'pointer',
            point: {
                events: {
                    click: function (e) {
                        hs.htmlExpand(null, {
                            pageOrigin: {
                                x: e.pageX || e.clientX,
                                y: e.pageY || e.clientY
                            },
                            headingText: this.series.name,
                            maincontentText: Highcharts.dateFormat('%A, %b %e, %Y', this.x) + ':<br/> ' +
                                this.y + ' sessions',
                            width: 200
                        });
                    }
                }
            },
            marker: {
                lineWidth: 1
            }
        }
    },

    series: [
      {
        name: 'AverageproductTemp',
        lineWidth: 4,
        marker: {
            radius: 4
        },
        data: xyPlots["productTemp"]
      },
      {
        name: 'Average process1Temp',
        lineWidth: 4,
        marker: {
            radius: 4
        },
        data: xyPlots["process1Temp"]
      },{
        name: 'Average process2Temp',
        lineWidth: 4,
        marker: {
            radius: 4
        },
        data: xyPlots["process2Temp"]
      },{
        name: 'Average waterTemp',
        lineWidth: 4,
        marker: {
            radius: 4
        },
        data: xyPlots["waterTemp"]
      }
    ]
  });

  Highcharts.chart('water', {

    chart: {
        scrollablePlotArea: {
            minWidth: 700
        }
    },

    // data: xyPlots['humidity'],

    title: {
        text: 'Average Process Water Flow of the Past Week'
    },

    xAxis: {
      type: 'datetime'
    },

    yAxis: [{ // left y axis
        title: {
            text: null
        },
        labels: {
            align: 'left',
            x: 3,
            y: 16,
            format: '{value:.,0f}'
        },
        showFirstLabel: false
    }, { // right y axis
        linkedTo: 0,
        gridLineWidth: 0,
        opposite: true,
        title: {
            text: null
        },
        labels: {
            align: 'right',
            x: -3,
            y: 16,
            format: '{value:.,0f}'
        },
        showFirstLabel: false
    }],

    legend: {
        align: 'left',
        verticalAlign: 'top',
        borderWidth: 0
    },

    tooltip: {
        shared: true,
        crosshairs: true
    },

    plotOptions: {
        series: {
            cursor: 'pointer',
            point: {
                events: {
                    click: function (e) {
                        hs.htmlExpand(null, {
                            pageOrigin: {
                                x: e.pageX || e.clientX,
                                y: e.pageY || e.clientY
                            },
                            headingText: this.series.name,
                            maincontentText: Highcharts.dateFormat('%A, %b %e, %Y', this.x) + ':<br/> ' +
                                this.y + ' sessions',
                            width: 200
                        });
                    }
                }
            },
            marker: {
                lineWidth: 1
            }
        }
    },

    series: [{
        name: 'Average Process Water Flow',
        lineWidth: 4,
        marker: {
            radius: 4
        },
        data: xyPlots['waterFlow']
    }]
  });

  Highcharts.chart('fan', {

    chart: {
        scrollablePlotArea: {
            minWidth: 700
        }
    },

    // data: xyPlots['humidity'],

    title: {
        text: 'Average Process Fan Intensity of the Past Week'
    },

    xAxis: {
      type: 'datetime'
    },

    yAxis: [{ // left y axis
        title: {
            text: null
        },
        labels: {
            align: 'left',
            x: 3,
            y: 16,
            format: '{value:.,0f}'
        },
        showFirstLabel: false
    }, { // right y axis
        linkedTo: 0,
        gridLineWidth: 0,
        opposite: true,
        title: {
            text: null
        },
        labels: {
            align: 'right',
            x: -3,
            y: 16,
            format: '{value:.,0f}'
        },
        showFirstLabel: false
    }],

    legend: {
        align: 'left',
        verticalAlign: 'top',
        borderWidth: 0
    },

    tooltip: {
        shared: true,
        crosshairs: true
    },

    plotOptions: {
        series: {
            cursor: 'pointer',
            point: {
                events: {
                    click: function (e) {
                        hs.htmlExpand(null, {
                            pageOrigin: {
                                x: e.pageX || e.clientX,
                                y: e.pageY || e.clientY
                            },
                            headingText: this.series.name,
                            maincontentText: Highcharts.dateFormat('%A, %b %e, %Y', this.x) + ':<br/> ' +
                                this.y + ' sessions',
                            width: 200
                        });
                    }
                }
            },
            marker: {
                lineWidth: 1
            }
        }
    },

    series: [{
        name: 'Average Process Fan Intensity',
        lineWidth: 4,
        marker: {
            radius: 4
        },
        data: xyPlots['fanIntensity']
    }]
  });

}
var yesterday = YESTERDAY
$.getJSON('/data/batch/yesterday/' + yesterday, disp_charts);

// /**
//  * In order to synchronize tooltips and crosshairs, override the
//  * built-in events with handlers defined on the parent element.
//  */
// ['mousemove', 'touchmove', 'touchstart'].forEach(function (eventType) {
//   document.getElementById('container').addEventListener(
//       eventType,
//       function (e) {
//           var chart,
//               point,
//               i,
//               event;

//           for (i = 0; i < Highcharts.charts.length; i = i + 1) {
//               chart = Highcharts.charts[i];
//               // Find coordinates within the chart
//               event = chart.pointer.normalize(e);
//               // Get the hovered point
//               point = chart.series[0].searchPoint(event, true);

//               if (point) {
//                   point.highlight(e);
//               }
//           }
//       }
//   );
// });

// /**
// * Override the reset function, we don't need to hide the tooltips and
// * crosshairs.
// */
// Highcharts.Pointer.prototype.reset = function () {
//   return undefined;
// };

// /**
// * Highlight a point by showing tooltip, setting hover state and draw crosshair
// */
// Highcharts.Point.prototype.highlight = function (event) {
//   event = this.series.chart.pointer.normalize(event);
//   this.onMouseOver(); // Show the hover marker
//   this.series.chart.tooltip.refresh(this); // Show the tooltip
//   this.series.chart.xAxis[0].drawCrosshair(event, this); // Show the crosshair
// };

// /**
// * Synchronize zooming through the setExtremes event handler.
// */
// function syncExtremes(e) {
//   var thisChart = this.chart;

//   if (e.trigger !== 'syncExtremes') { // Prevent feedback loop
//       Highcharts.each(Highcharts.charts, function (chart) {
//           if (chart !== thisChart) {
//               if (chart.xAxis[0].setExtremes) { // It is null while updating
//                   chart.xAxis[0].setExtremes(
//                       e.min,
//                       e.max,
//                       undefined,
//                       false,
//                       { trigger: 'syncExtremes' }
//                   );
//               }
//           }
//       });
//   }
// }

// // Get the data. The contents of the data file can be viewed at
// Highcharts.ajax({
//   url: 'https://cdn.jsdelivr.net/gh/highcharts/highcharts@v7.0.0/samples/data/activity.json',
//   dataType: 'text',
//   success: function (activity) {

//       activity = JSON.parse(activity);
//       activity.datasets.forEach(function (dataset, i) {

//           // Add X values
//           dataset.data = Highcharts.map(dataset.data, function (val, j) {
//               return [activity.xData[j], val];
//           });

//           var chartDiv = document.createElement('div');
//           chartDiv.className = 'chart';
//           document.getElementById('container').appendChild(chartDiv);

//           Highcharts.chart(chartDiv, {
//               chart: {
//                   marginLeft: 40, // Keep all charts left aligned
//                   spacingTop: 20,
//                   spacingBottom: 20
//               },
//               title: {
//                   text: dataset.name,
//                   align: 'left',
//                   margin: 0,
//                   x: 30
//               },
//               credits: {
//                   enabled: false
//               },
//               legend: {
//                   enabled: false
//               },
//               xAxis: {
//                   crosshair: true,
//                   events: {
//                       setExtremes: syncExtremes
//                   },
//                   labels: {
//                       format: '{value} km'
//                   }
//               },
//               yAxis: {
//                   title: {
//                       text: null
//                   }
//               },
//               tooltip: {
//                   positioner: function () {
//                       return {
//                           // right aligned
//                           x: this.chart.chartWidth - this.label.width,
//                           y: 10 // align to title
//                       };
//                   },
//                   borderWidth: 0,
//                   backgroundColor: 'none',
//                   pointFormat: '{point.y}',
//                   headerFormat: '',
//                   shadow: false,
//                   style: {
//                       fontSize: '18px'
//                   },
//                   valueDecimals: dataset.valueDecimals
//               },
//               series: [{
//                   data: dataset.data,
//                   name: dataset.name,
//                   type: dataset.type,
//                   color: Highcharts.getOptions().colors[i],
//                   fillOpacity: 0.3,
//                   tooltip: {
//                       valueSuffix: ' ' + dataset.unit
//                   }
//               }]
//           });
//       });
//   }
// });
