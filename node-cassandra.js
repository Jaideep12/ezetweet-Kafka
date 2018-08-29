//Sample code for connecting to apache cassandra and inserting records into the table, The code is primarily responsible for taking user details
//supplied by kafka and sending those details into the cassandra database.

const cassandra = require('cassandra-driver');
const client = new cassandra.Client({contactPoints:['127.0.0.1'],keyspace: 'node' });
var assert=require('assert');

  var username='abcd.efgh';
  var name='Patrick robinson';
  var email='pat123@gmail.com';
  var v=123;


  const query2="INSERT INTO users(username,name,email,v) values("+"'"+username+"'"+","+"'"+name+"'"+","+"'"+email+"'"+","+v+")";
  client.execute(query2, function(err, result) {
  assert.ifError(err);
  console.log('Data inserted into the cassanrda database');
});