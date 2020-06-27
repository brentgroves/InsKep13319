const mariadb = require("mariadb");
const mqtt = require('mqtt');
const common = require('@bgroves/common');




const {
  MQTT_SERVER,
  MYSQL_HOSTNAME,
  MYSQL_USERNAME,
  MYSQL_PASSWORD,
  MYSQL_DATABASE
} = process.env;
/*
const MQTT_SERVER='localhost';
const MYSQL_HOSTNAME= "localhost";
const MYSQL_USERNAME= "brent";
const MYSQL_PASSWORD= "JesusLives1!";
const MYSQL_DATABASE= "mach2";
*/
const connectionString = {
  connectionLimit: 5,
  multipleStatements: true,
  host: MYSQL_HOSTNAME,
  user: MYSQL_USERNAME,
  password: MYSQL_PASSWORD,
  database: MYSQL_DATABASE
}

common.log(`user: ${MYSQL_USERNAME},password: ${MYSQL_PASSWORD}, database: ${MYSQL_DATABASE}, MYSQL_HOSTNAME: ${MYSQL_HOSTNAME}`);

const pool = mariadb.createPool( connectionString);



async function InsKep13319(nodeId,plexus_Customer_No,pcn, workcenter_Key,workcenter_Code,cnc,cycle_Counter_Shift_SL,transDate) {
  let conn;
  try {
    conn = await pool.getConnection();      
    const someRows = await conn.query('call InsKep13319(?,?,?,?,?,?,?,?)',[nodeId,plexus_Customer_No,pcn, workcenter_Key,workcenter_Code,cnc,cycle_Counter_Shift_SL,transDate]);
    let msgString = JSON.stringify(someRows[0]);
    const obj = JSON.parse(msgString.toString()); // payload is a buffer
    common.log(obj);
  } catch (err) {
    // handle the error
    console.log(`Error =>${err}`);
  } finally {
    if (conn) conn.release(); //release to pool
  }
}

//InsKep13319('test', 123,'Avilla',123, 'CNC103', '103', 5, '2020-06-25 00:00:00');

function main() {
  common.log(`MQTT_SERVER=${MQTT_SERVER}`);
  const mqttClient = mqtt.connect(`mqtt://${MQTT_SERVER}`);

  mqttClient.on('connect', function() {
    mqttClient.subscribe('Kep13319', function(err) {
      if (!err) {
        common.log('InsKep13319 has subscribed to: Kep13319');
      }
    });
  });

  // message is a buffer
  mqttClient.on('message', function(topic, message) {
    const obj = JSON.parse(message.toString()); // payload is a buffer

    common.log(`InsKep13319 => ${message.toString()}`);
    InsKep13319(obj.nodeId,obj.plexus_Customer_No,obj.pcn,obj.workcenter_Key,obj.workcenter_Code,obj.cnc,obj.cycle_Counter_Shift_SL,obj.transDate);

  });
}
main();
/*
        let msg = {
          nodeId: config.NodeId[i].NodeId,
          plexus_Customer_No: config.NodeId[i].Plexus_Customer_No,
          pcn: config.NodeId[i].PCN,
          workcenter_Key: config.NodeId[i].Workcenter_Key,
          workcenter_Code: config.NodeId[i].Workcenter_Code,
          cnc: config.NodeId[i].CNC,
          cycle_Counter_Shift_SL: cycle_Counter_Shift_SL,
          transDate: transDate
        };
*/