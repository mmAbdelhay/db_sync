require("dotenv").config();
const mysql = require("mysql2/promise");
const { exec } = require("child_process");
const _ = require("lodash");

console.time("syncing");

(async () => {
  exec("perl --version", (error, stdout, stderr) => {
    if (error) {
      console.error(`Error checking for Perl: ${error.message}`);
      process.exit(1);
    }
    if (stderr) {
      console.error(`Error checking for Perl: ${stderr}`);
      process.exit(1);
    }
    // Check the output for the Perl version number
    const perlVersionMatch = stdout.match(/This is perl/);
    if (perlVersionMatch) {
      console.log(`Perl is installed`);
    } else {
      console.log(`Perl is not installed`);
      process.exit(1);
    }
  });

  const srcConnection = await mysql.createConnection({
    host: process.env.src_host,
    user: process.env.src_user,
    port: process.env.src_port,
    password: process.env.src_password,
    database: process.env.src_db,
  });

  const tgtConncetion = await mysql.createConnection({
    host: process.env.tgt_host,
    port: process.env.tgt_port,
    user: process.env.tgt_user,
    password: process.env.tgt_password,
  });

  const targetDbExist = await tgtConncetion.execute("CREATE DATABASE IF NOT EXISTS `" + process.env.tgt_db + "`");

  if (targetDbExist) {
    await tgtConncetion.end();

    const newTgtConnection = await mysql.createConnection({
      host: process.env.tgt_host,
      port: process.env.tgt_port,
      user: process.env.tgt_user,
      password: process.env.tgt_password,
      database: process.env.tgt_db,
    });

    const [results] = await srcConnection.execute("SHOW TABLES;");
    let tables = [];
    Object.entries(results).forEach((obj) => {
      tables.push(Object.values(obj[1])[0]);
    });
    tables.forEach(async (table) => {
      const [createStatementFromDB] = await srcConnection.execute(`SHOW CREATE TABLE ${table} ;`);
      const createStatement = Object.entries(createStatementFromDB[0])[1][1];
      const testStatement = createStatement.split("CREATE TABLE `" + table + "`")[1];
      await newTgtConnection.execute("CREATE TABLE IF NOT EXISTS `" + table + "` " + testStatement);

      const [columns] = await srcConnection.execute(
        `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '${process.env.src_db}' AND TABLE_NAME = '${table}';`
      );
      const conflictColumn = columns[0].COLUMN_NAME;

      exec(
        `perl pt-table-sync-local.pl --execute --verbose --bidirectional --conflict-column ${conflictColumn}  --conflict-comparison newest h=${process.env.src_host},P=${process.env.src_port},u=${process.env.src_user},p=${process.env.src_password},D=${process.env.src_db},t=${table}, h=${process.env.tgt_host},P=${process.env.tgt_port},u=${process.env.tgt_user},p=${process.env.tgt_password},D=${process.env.tgt_db},t=${table}`,
        (error, stdout, stderr) => {
          if (error) {
            console.error(`pt-table-sync error: ${error.message}`);
          }
          if (stderr) {
            console.error(`pt-table-sync stderr: ${stderr}`);
            return;
          }
          console.log(`pt-table-sync stdout: ${stdout}`);
          console.timeLog("syncing");
        }
      );
    });
  }
})();
