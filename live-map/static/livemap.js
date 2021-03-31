// Leaflet library: para criar mapas interativos
const myMap = L.map('mapid').setView([51.505, -0.09], 13);

L.tileLayer('https://api.mapbox.com/styles/v1/{id}/tiles/{z}/{x}/{y}?access_token={accessToken}', {
  attribution: 'Map data &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, Imagery © <a href="https://www.mapbox.com/">Mapbox</a>',
  maxZoom: 18,
  id: 'mapbox/streets-v11',
  tileSize: 512,
  zoomOffset: -1,
  accessToken: 'pk.eyJ1Ijoicm9iZXJ0b21vcmVsIiwiYSI6ImNrbGZsY2NiODA4OXgycXM3NXMzOG0yZHMifQ.5ff5mArLAVl2j6t9744bvA'
}).addTo(myMap);

let mapMarkers1 = [];

const source = new EventSource('/topic/busao'); // NOME DO TOPICO
source.addEventListener('message', function(e) {
  console.log('Message');
  obj = JSON.parse(e.data);
  console.log(obj);

  if(obj.busline === '00001') {
    for (var i = 0; i < mapMarkers1.length; i++) {
      mymap.removeLayer(mapMarkers1[i]);
    }

    marker1 = L.marker([obj.latitude, obj.longitude]).addTo(mymap);
    mapMarkers1.push(marker1);
  }
}, false);