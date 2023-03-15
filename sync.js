require("dotenv").config();
const mysql = require("mysql2");

const src_db = process.env.src_db;
const tgt_db = process.env.tgt_db;

const sourceConnection = mysql.createConnection({
  host: process.env.src_host,
  user: process.env.src_user,
  password: process.env.src_password,
  database: src_db,
});

const targetConnection = mysql.createConnection({
  host: process.env.tgt_host,
  user: process.env.tgt_user,
  password: process.env.tgt_password,
  database: tgt_db,
});

sourceConnection.query(`show tables;`, (err, results) => {
  if (err) {
    console.error(err);
  } else {
    let tables = [];
    Object.entries(results).forEach((obj) => {
      tables.push(Object.values(obj[1])[0]);
    });
    tables.forEach((table) => {
      targetConnection.query(
        "CREATE TABLE IF NOT EXISTS `" + tgt_db + "`.`" + table + "` AS SELECT * FROM `" + src_db + "`.`" + table + "`"
      );
    });
    console.log("sync is complete");
  }
});
