var express = require('express');
var kafka = require('kafka-node');
var app = express();

var bodyParser = require('body-parser')
app.use(bodyParser.json()); 
app.use(bodyParser.urlencoded({     
  extended: true
})); 


//Configuring the producer for sending data 
var Producer = kafka.Producer,
    client = new kafka.Client(),
    producer = new Producer(client);

//Defining event handlers for specific events
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

//----------------------------------------------------------
//app.post('/sendMsg',function(req,res){
    //var config = require('./test.json');
    //res.setHeader('Content-Type', 'application/json');
    //for(var i=0;i<config.length;i++)
   //{
      //var sentMessage = JSON.stringify(config.message);

      //---------------------------------------------------------

      //Creating a sample JSON which is to be send

      var data={
        Name:"Jaideep Singh",
        message:"Sending directly through the payloads",
        Emp_id:"1000",
        Emp_designation:"SW"
      };

      //Code for sending the message to the particular topic
      var sent=JSON.stringify(data);
      payloads = [
          { topic:'Whi', messages:['"Message from Whi"'], partition: 0 }
      ];
      producer.on('ready',function(){
       producer.send(payloads, function (err, data) {
             console.log(data);
      });
     });

      //----------------------------------------------------------------
     // }   
//}
//});