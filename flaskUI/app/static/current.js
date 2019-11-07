const processIsOnTranslate = { '1': 'ON', '0': 'OFF' };
const avgKeys = ['inputTemperatureProduct', 
                  'waterFlowProcess',
                  'intensityFanProcess',
                  'waterTemperatureProcess',
                  'temperatureProcess1',
                  'temperatureProcess2'];


function writeAvgToWeb(avgData) {
  avgKey = avgData[0];
  avgVal = parseFloat(avgData[1]).toFixed(2);
  console.log(avgKey, "||", avgVal);

  if (avgKey == avgKeys[0]) {
    $('#avginputTemperatureProduct').text(avgVal);
  } else if (avgKey == avgKeys[1]) {
    $('#avgWaterFlowProcess').text(avgVal);
  } else if (avgKey == avgKeys[2]) {
    $('#avgIntensityFanProcess').text(avgVal);
  } else if (avgKey == avgKeys[3]) {
    $('#avgWaterTemperatureProcess').text(avgVal);
  } else if (avgKey == avgKeys[4]) {
    $('#avgTemperatureProcess1').text(avgVal);
  } else if (avgKey == avgKeys[5]) {
    $('#avgTemperatureProcess2').text(avgVal);
  }

  lastTimestamp = document.getElementById("avgTimestamp");
  formattedDt = formatDt(new Date())
  if (lastTimestamp == null || lastTimestamp.innerHTML != formattedDt) {
    $('#avgTimestamp').text(formattedDt);
    console.log("new timestamp", formattedDt);
  }
}

function writeCurrToWeb(currData) {
  $('#processIsOn').text(processIsOnTranslate[currData[1]]);
  $('#inputTemperatureProduct').text(parseFloat(currData[2]).toFixed(2));
  $('#waterFlowProcess').text(parseFloat(currData[3]).toFixed(2));
  $('#intensityFanProcess').text(parseFloat(currData[4]).toFixed(2));
  $('#waterTemperatureProcess').text(parseFloat(currData[5]).toFixed(2));
  $('#temperatureProcess1').text(parseFloat(currData[6]).toFixed(2));
  $('#temperatureProcess2').text(parseFloat(currData[7]).toFixed(2));

  formattedDt = formatDt(new Date());
  $('#currTimestamp').text(formattedDt);
}

// Render data from session in case of refresh
for (let key of avgKeys) {
  avgData = localStorage.getItem(key).split(",");
  if (avgData) {
    writeAvgToWeb(avgData);
  }
}

currData = localStorage.getItem("curr")
if (currData) {
  writeCurrToWeb(currData);
}


function formatDt(timestampStr) {
  dt = new Date(timestampStr);
  return dt.toLocaleString('en-US', { timeZone: "America/New_York" })
} 

var currSource = new EventSource('/data/sensor');
currSource.onmessage = function(event) {
  currData = event.data.split(',');
  localStorage.setItem("curr", currData)
  writeCurrToWeb(currData);
};

var avgSource = new EventSource('/data/averages');
avgSource.onmessage = function(event) {
  avgData = event.data.split(',');
  localStorage.setItem(avgData[0], avgData);
  writeAvgToWeb(avgData);

};

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

const HBASE_COLUMNS = ["inputTemperatureProduct", "temperatureProcess1", 
                        "temperatureProcess2", "waterTemperatureProcess",
                        "waterFlowProcess", "intensityFanProcess"];
const HBASE_CF = 'METER'
const COLUMNS = ["productTemp", "process1Temp", "process2Temp", "waterTemp", "waterFlow", "fanIntensity"]

function extractValues(items) {
  var xyPlots = {"productTemp": [],
                 "process1Temp": [],
                 "process2Temp": [],
                 "waterTemp": [],
                 "waterFlow": [],
                 "fanIntensity": []}

  for (let row of items) {
    var rowkey = row[0]
    var values = row[1]
    var tstamp = rowkey.split("#")[2] // yyyyMMddHHmm

    var year = Number(tstamp.substring(0, 4));
    var monthIndex = Number(tstamp.substring(4, 6)) - 1;
    var day = Number(tstamp.substring(6, 8));
    var hours = Number(tstamp.substring(8, 10));
    var minutes = Number(tstamp.substring(10, 12));
    var xVal = new Date(year, monthIndex, day, hours, minutes).getTime();

    for (var i=0; i < HBASE_COLUMNS.length; i++) {
      // console.log(HBASE_CF + ":" + HBASE_COLUMNS[i])
      // console.log(values[HBASE_CF + ":" + HBASE_COLUMNS[i]], values)
      yVal = values[HBASE_CF + ":" + HBASE_COLUMNS[i]]
      xyPlots[COLUMNS[i]].push([xVal, yVal])
    }
  }
    // must sort the list by order of time, or else HighCharts doesn't understand
  for (let key of COLUMNS) {
    xyPlots[key] = xyPlots[key].sort((a, b) => (a[0] < b[0]) ? 1 : -1)
  }
  return xyPlots
}

function disp_avg_chart(data) {
  xyPlots = extractValues(data.items)
  // console.log(data.items)
  // console.log("PLOTPOINTS: ", xyPlots) 

  Highcharts.chart('temperatures', {

    chart: {
        scrollablePlotArea: {
            minWidth: 700
        }
    },

    title: {
        text: 'Average Temperatures of the Past 24 Hours'
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
        name: 'productTemp',
        lineWidth: 4,
        marker: {
            radius: 4
        },
        data: xyPlots["productTemp"]
      },
      {
        name: 'process1Temp',
        lineWidth: 4,
        marker: {
            radius: 4
        },
        data: xyPlots["process1Temp"]
      },{
        name: 'process2Temp',
        lineWidth: 4,
        marker: {
            radius: 4
        },
        data: xyPlots["process2Temp"]
      },{
        name: 'waterTemp',
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

    title: {
        text: 'Average Water Flow of the 24 Hours'
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
        name: 'waterFlow',
        lineWidth: 4,
        marker: {
            radius: 4
        },
        data: xyPlots["waterFlow"]
      }
    ]
  });

  Highcharts.chart('fan', {

    chart: {
        scrollablePlotArea: {
            minWidth: 700
        }
    },

    title: {
        text: 'Average Fan Intensity of the 24 Hours'
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
        name: 'fanIntensity',
        lineWidth: 4,
        marker: {
            radius: 4
        },
        data: xyPlots["fanIntensity"]
      }
    ]
  });
}


$.getJSON('/data/past24sim', disp_avg_chart);
