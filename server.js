var express = require('express');
var app = express();
uuid = require('node-uuid');
 
app.get('/', function (req, res) {
  var eTag = req.headers['if-none-match'];

  if(!eTag) {
  	eTag = uuid.v4()+uuid.v4();
  }
  res.set({"ETag":eTag,"Content-Type": "text/plain","Cache-Control":"must-revalidate, post-check=0, pre-check=0","Last-Modified": (new Date()).toUTCString()});
  res.send('Your Device ID: '+eTag);
})
 
app.listen(9000);