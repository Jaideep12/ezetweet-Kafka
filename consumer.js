    var kafka = require('kafka-node');
    var fs=require('fs');
    var topics=[{topic:'Kafka_one'}];
    var Consumer = kafka.Consumer;
    var client = new kafka.Client();
    var consumer = new Consumer(client,[{topic:'Kafka_one',partition:0},{topic:'Asc',partition:0},{topic:'Whi',partition:0},{topic:'Hack',partition:0}],
        {
            autoCommit: false,
            fetchMaxBytes: 1024 * 1024,
            encoding:"buffer"
        }
);

//Executing linux command in node.js
var flag;

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

  var array=data2.split(',');
  var i,k,j=0;

//Searching for data from the configuration file and retrieving the relevant topics.
  for(k=0;k<array5.length;k++)
  {
    console.log("Now array 5 is = "+array5[k]);
    for(i=0;i<array.length;i++)
    {
      console.log("Now array is = "+array[i]);
      if(array5[k].indexOf(array[i])>-1 || array5[k]==array[i])
      {
        var topic1=new Array();
        topic1[j]=array5[k]
        console.log("The topic found is="+topic1[j]);
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

var array4 = fs.readFileSync('output.txt').toString().split("\n");
var len=array4.length;
var i,l=0;
var array5=new Array();
for(i=0;i<len;i++)
{
   if(array4[i].charAt(0)!='_')
   {
      array5[l]=array4[i];
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
	      var buf = new Buffer(message.value, "binary"); 
        var decodedMessage = JSON.parse(buf.toString()); 
	      console.log(decodedMessage);
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