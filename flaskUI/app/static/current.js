const processIsOnTranslate = { '1': 'ON', '0': 'OFF' };

function formatDt(timestampStr) {
  dt = new Date(timestampStr);
  return dt.toLocaleString('en-US', { timeZone: "America/New_York" })
} 

var currSource = new EventSource('/data/sensor');
currSource.onmessage = function(event) {
  currData = event.data.split(',');
  $('#processIsOn').text(processIsOnTranslate[currData[1]]);
  $('#inputTemperatureProduct').text(parseFloat(currData[2]).toFixed(2));
  $('#waterFlowProcess').text(parseFloat(currData[3]).toFixed(2));
  $('#intensityFanProcess').text(parseFloat(currData[4]).toFixed(2));
  $('#waterTemperatureProcess').text(parseFloat(currData[5]).toFixed(2));
  $('#temperatureProcess1').text(parseFloat(currData[6]).toFixed(2));
  $('#temperatureProcess2').text(parseFloat(currData[7]).toFixed(2));

  formattedDt = formatDt(new Date());
  $('#currTimestamp').text(formattedDt);
  console.log(currData, formattedDt);
};

const avgKeys =
  ['inputTemperatureProduct', 
  'waterFlowProcess',
  'intensityFanProcess',
  'waterTemperatureProcess',
  'temperatureProcess1',
  'temperatureProcess2'];

var avgSource = new EventSource('/data/averages');
avgSource.onmessage = function(event) {
  avgData = event.data.split(',');
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
};
