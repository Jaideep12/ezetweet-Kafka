//Consumer code for taking data from the respective kafka topics and making it available for further manipulation
//The code first constructs anoutput file by executing unix commands through node.js and then converting the file into a list
//After which tehe list is compared with another configuration file containing prefixes of data sources signifying which topics to consider


        // var cassandra = require('cassandra-driver');
       //var client3 = new cassandra.Client({contactPoints:['127.0.0.1'],keyspace: 'node' });

       
    var topic_list=[];

    var MongoClient = require('mongodb').MongoClient;

    var redis = require('redis');
    var client2 = redis.createClient();
    var assert=require('assert');
    var kafka = require('kafka-node');
    var fs=require('fs');
    var topics=[{topic:'Kafka_one'}];
    var Consumer = kafka.Consumer;
    var client = new kafka.Client();
    var consumer = new Consumer(client,[{topic:'Ezest-Whiziblem-connect-MileStone',partition:0}],
        {
            autoCommit: false,
            fetchMaxBytes: 1024 * 1024
        }
);

//var topic_list=[];
//Executing linux command in node.js

var flag=1;
var current_topic="Ezest-Whiziblem-connect-MileStone";

//Exceuting unix command for navigating into the particular directory
var exec=require("child_process").exec;
exec("cd /home/jaideep/confluent-oss-5.0.0-2.11/confluent-5.0.0",function(err,stdout)
{
  if(err)
    throw err;
  else
  {
    flag=0;  
    console.log("Finished executing first");
    console.log("Value of flag = "+flag);
    get_list();
  }
})

//Executing unix command for getting the list of all kafka topics from the zookeeper
function get_list()
{
   console.log("Entering the second statement");
   exec("/home/jaideep/confluent-oss-5.0.0-2.11/confluent-5.0.0/bin/kafka-topics --zookeeper localhost:2181 -list > /home/jaideep/myapp/output.txt",function(err,stdout)
 {
  if(err)
    throw err;

  console.log("Finished executing second");
  var array_output = fs.readFileSync('/home/jaideep/myapp/output.txt').toString().split("\n");
  console.log(stdout);
})
}

//Reading data from a configuration file for finding topics from which data is to be read by kafka

fs.readFile('topic_config','utf8', function (err, data2) {
  if (err) throw err;
  
  var array_config=data2.split(',');
  var i,k,j=0;

//Searching for data from the configuration file and retrieving the relevant topics.
  for(k=0;k<array_filter.length;k++)
  {
    for(i=0;i<array_config.length;i++)
    {
      if(array_filter[k].indexOf(array_config[i])>-1 || array_filter[k]==array_config[i])
      {
        topic_list[j]=array_filter[k];
        console.log("Topic = "+topic_list[j]);
        j++;
        break;
      }
    }
  }
});

//----------------------------------------------------
//Formation of file for topics of kafka
/*fs.readFile('output.txt','utf8',function(err,data3){
  if(err)
    throw err;
});*/
//------------------------------------------------------


//Converting the file containing the list of all kafka topics into a list

var array_output = fs.readFileSync('output.txt').toString().split("\n");
var len=array_output.length;
var i,l=0;
var array_filter=new Array();
for(i=0;i<len;i++)
{
   if(array_output[i].charAt(0)!='_')
   {
      array_filter[l]=array_output[i];
      l++;
   }
}
//---------------------------------------------------------
/*console.log("Array 5 is");
console.log("Length of array 5 is ="+array5.length);
for(m in array5)
{
  console.log(array5[m]);
}*/
//---------------------------------------------------------

//Displaying data read by kafka consumer on NODE console 
consumer.on('message', function (message) {
	if(message.offset!=0)
	{
        Message_formation(message.value);
	 }
});

function Message_formation(message)
{
    var jsonParsed = JSON.parse(message);
    // access elements
    var field=jsonParsed.schema.fields;
    for(i=0;i<field.length;i++)
    {
        var name=field[i].name;
        if(name!=undefined)
        {
          if(name.includes("Timestamp"))
          {
            var temp=field[i].field;
            var val=jsonParsed.payload[temp];
            if(val!=null)
            {
              var final_date=new Date(val);
              jsonParsed.payload[temp]=final_date;
            }
          }
        }
    }
    var msg=JSON.stringify(jsonParsed);

    //send_to_cassandra(b);
    var final_msg=filter_templates(msg);
    //console.log(final_msg);
    //send_to_redis(b);

}
function filter_templates(message)
{
   MongoClient.connect("mongodb://172.52.90.34:27017/", function(err, db) {
   if(!err) {
        
      var dbo=db.db("TestServerSimpleTwitter");

     var cursor = dbo.collection('events').find();
     {
         cursor.each(function(err, doc) {

               if(doc!=undefined)
               {
                  if(current_topic.search(doc.parent_name))
                  {
                    var event_message=doc.event_message;
                    var fields=event_message.split('#');
                    var template_array=[];
                    var counter_template=0;
                    for(i=0;i<fields.length;i++)
                    {
                       if(fields[i]!=" " && fields[i]!=undefined)
                       {
                         template_array[counter_template++]=fields[i];
                       }        
                    }
                    var object={};
                    for(j=0;j<template_array.length;j=j+2)
                    {
                        var jsonParsed = JSON.parse(message);
                        var key = template_array[j];
                        if(typeof(jsonParsed.payload[template_array[j]])!='undefined' && typeof(jsonParsed.payload[template_array[j]])!='null')
                        {
                           object[key]=jsonParsed.payload[template_array[j]];
                        }
                    }
                    var msg=JSON.stringify(object); 
                    console.log("MESSAGE = "+object);
                    /*
                    if(msg!=null)
                    {
                      send_to_redis(message,msg);
                     }*/ 
                  }
               }
         });
     }
   }
});
}

function send_to_redis(message,msg)
{
  var jsonParsed = JSON.parse(message);
  var id=jsonParsed.payload.ID;
  var key=id.toString();
  console.log(typeof(key));
  client2.on('connect', function() {
    console.log('connected');
  });
  client2.sadd(['jjj',message,msg], function(err, reply) {
    console.log(reply); // 3
});
  client2.smembers('jjj', function(err, reply) {
    console.log(reply);
  });
}

function send_to_cassandra(message)
{
  const query="INSERT INTO Messages(message) values("+"'"+message+"'"+")";
  client3.execute(query, function(err, result) {
  assert.ifError(err);
  console.log('Data inserted into the cassandra database');
});

}

consumer.on('error', function (err) {
    console.log('Error:',err);
})

consumer.on('offsetOutOfRange', function (err) {
    console.log('offsetOutOfRange:',err);
})