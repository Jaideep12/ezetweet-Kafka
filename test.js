var express = require('express');
var kafka = require('kafka-node');
var app = express();

var bodyParser = require('body-parser')
app.use(bodyParser.json()); 
app.use(bodyParser.urlencoded({     
  extended: true
})); 

var Producer = kafka.Producer,
    client = new kafka.Client(),
    producer = new Producer(client);

producer.on('ready', function () {
    console.log('Producer is ready');
});

producer.on('error', function (err) {
    console.log('Producer is in error state');
    console.log(err);
})
app.get('/',function(req,res){
    res.json({greeting:'Kafka Producer'})
});

app.listen(5001,function(){
    console.log('Kafka producer running at 5001')
});

//app.get('/sendMsg',function(req,res){
    //var config = require('./test.json');
    //res.setHeader('Content-Type', 'application/json');
    //for(var i=0;i<config.length;i++)
   //{
      //var sentMessage = JSON.stringify(config.message);
      var data={
        Name:"Jaideep Singh",
        message:"Sending directly through the payloads",
        Emp_id:"1000",
        Emp_designation:"SW"
      };
      var sent=JSON.stringify(data);
      payloads = [
          { topic:'Hack', messages:['"Message from Hackethon"'], partition: 0 }
      ];
      producer.on('ready',function(){
       producer.send(payloads, function (err, data) {
             console.log(data);
      });
     });
     // }   
