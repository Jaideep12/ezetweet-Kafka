//Consumer code for taking data from the respective kafka topics and making it available for further manipulation
//The code first constructs anoutput file by executing unix commands through node.js and then converting the file into a list
//After which tehe list is compared with another configuration file containing prefixes of data sources signifying which topics to consider

    var kafka = require('kafka-node');
    var fs=require('fs');
    var topics=[{topic:'Kafka_one'}];
    var Consumer = kafka.Consumer;
    var client = new kafka.Client();
    var consumer = new Consumer(client,[{topic:'Parent_data',partition:0}],
        {
            autoCommit: false,
            fetchMaxBytes: 1024 * 1024,
           // encoding:"buffer"
        }
);

//Executing linux command in node.js

var flag;

//Exceuting unix command for navigating into the particular directory
var exec=require("child_process").exec;
exec("cd /opt/confluent-kafka/confluent-5.0.0/etc/kafka",function(err,stdout)
{
  if(err)
    throw err;
  else
  {
    flag=0;  
    console.log("Finished executing first");
  }
})

//Executing unix command for getting the list of all kafka topics from the zookeeper
if(flag==0)
{
exec("/opt/confluent-kafka/confluent-5.0.0/bin/kafka-topics --zookeeper server:2181 -list > output.txt",function(err,stdout)
{
  if(err)
    throw err;

  console.log("Finished executing second");
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
        var topic_list=new Array();
        topic_list[j]=array_filter[k]
        //console.log("The topic found is="+topic_list[j]);
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
	if(message.offset!=0 && message.value!=null)
	{
        var buf = new Buffer(JSON.stringify(message)); 
        var decodedMessage = JSON.parse(buf.toString()); 
        console.log(decodedMessage.value);
        console.log("-------------------------------------------------------------------------");
	      consumer.close();
	 }
});

consumer.on('error', function (err) {
    console.log('Error:',err);
})

consumer.on('offsetOutOfRange', function (err) {
    console.log('offsetOutOfRange:',err);
})
