require("dotenv").config();
const mysql = require("mysql2");

// set up master database connection
const masterConnection = mysql.createConnection({
  host: process.env.src_host,
  user: process.env.src_user,
  password: process.env.src_password,
  database: process.env.src_db,
});

// set up slave database connection
const slaveConnection = mysql.createConnection({
  host: process.env.tgt_host,
  user: process.env.tgt_user,
  password: process.env.tgt_password,
  //   database: process.env.src_host,
});

// configure replication parameters for slave
slaveConnection.query(
  ` CHANGE MASTER TO MASTER_HOST=${process.env.src_host}, MASTER_USER=${process.env.src_user}, MASTER_PASSWORD=${process.env.src_password}, MASTER_LOG_FILE='mysql-bin.000001'`
);

// start replication on slave
slaveConnection.query("START SLAVE");

// MASTER_LOG_POS = 107; // adjust to match master's binary log position
