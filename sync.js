require("dotenv").config();
const mysql = require("mysql2");
const child_process = require("child_process");

console.time("syncing");

const src_db = process.env.src_db;
const tgt_db = process.env.tgt_db;

child_process.exec("perl --version", (error, stdout, stderr) => {
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

const sourceConnection = mysql.createConnection({
  host: process.env.src_host,
  user: process.env.src_user,
  port: process.env.src_port,
  password: process.env.src_password,
  database: src_db,
});

const targetConnection = mysql.createConnection({
  host: process.env.tgt_host,
  port: process.env.tgt_port,
  user: process.env.tgt_user,
  password: process.env.tgt_password,
});

targetConnection.query("CREATE DATABASE IF NOT EXISTS `" + tgt_db + "`", (err, result) => {
  if (err) throw err;

  const newTargetConnection = mysql.createConnection({
    host: process.env.tgt_host,
    port: process.env.tgt_port,
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
      tables.forEach((table, index) => {
        sourceConnection.query(`show create table ${table}`, (error, results, fields) => {
          if (error) throw error;

          const createStatement = Object.entries(results[0])[1][1];
          const test = createStatement.split("CREATE TABLE `" + table + "`")[1];

          // Execute the CREATE TABLE statement on the target host
          newTargetConnection.query("CREATE TABLE IF NOT EXISTS `" + table + "` " + test, (error, results, fields) => {
            if (error) {
              console.log(error);
            }

            checkSumAndSync(table);

            // child_process.exec(
            //   `perl pt-table-sync-local.pl --execute --no-check-slave --verbose --no-unique-checks h=${process.env.src_host},P=${process.env.src_port},u=${process.env.src_user},p=${process.env.src_password},D=${process.env.src_db},t=${table} h=${process.env.tgt_host},P=${process.env.tgt_port},u=${process.env.tgt_user},p=${process.env.tgt_password},D=${process.env.tgt_db}`,
            //   (err, stdout, stderr) => {
            //     if (err) {
            //       console.error(stderr);
            //     } else {
            //       console.log(stdout);
            //       console.timeLog("syncing");
            //     }
            //   }
            // );
          });
        });
      });
    }
  });
});

function checkSumAndSync(table) {
  // Run pt-table-checksum command
  const checksumCommand = `perl pt-table-checksum-local.pl --host=${process.env.src_host} --user=${process.env.src_user} --password=${process.env.src_password} --databases ${process.env.src_db} --tables ${table} --replicate ${process.env.src_db}.checksums --chunk-size=1000 --create-replicate-table --empty-replicate-table --no-check-binlog-format`;

  child_process.exec(checksumCommand, (err, stdout, stderr) => {
    if (err) {
      console.error(`Error running pt-table-checksum: ${err}`);
      return;
    }

    console.log(`pt-table-checksum output: ${stdout}`);

    // Run pt-table-sync command
    const syncCommand = `perl pt-table-sync-local.pl --host=${process.env.tgt_host} --user=${process.env.tgt_user} --password=${process.env.tgt_password} --replicate ${process.env.tgt_db}.checksums --sync-to-master h=${process.env.src_host},P=${process.env.src_port},u=${process.env.src_user},p=${process.env.src_password},D=${process.env.src_db},t=${table} h=${process.env.tgt_host},P=${process.env.tgt_port},u=${process.env.tgt_user},p=${process.env.tgt_password},D=${process.env.tgt_db},t=${table}`;

    child_process.exec(syncCommand, (err, stdout, stderr) => {
      if (err) {
        console.error(`Error running pt-table-sync: ${err}`);
        return;
      }

      console.log(`pt-table-sync output: ${stdout}`);
    });
  });
}
