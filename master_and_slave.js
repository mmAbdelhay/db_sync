require("dotenv").config();
const mysql = require("mysql2/promise");
const { exec } = require("child_process");

const newUser = process.env.newuser;
const newPassword = process.env.newpassword;

(async () => {
  // Connect to the master and slave servers
  const masterConnection = await mysql.createConnection({
    host: process.env.src_host,
    user: process.env.src_user,
    port: process.env.src_port,
    password: process.env.src_password,
    database: process.nextTick.src_db,
    authPlugins: {
      mysql_clear_password: () => () => Buffer.from(process.env.src_password), // use the 'mysql_clear_password' plugin to send the password in plain text
    },
  });

  const slaveConnection = await mysql.createConnection({
    host: process.env.tgt_host,
    port: process.env.tgt_port,
    user: process.env.tgt_user,
    password: process.env.tgt_password,
  });

  const targetDbExist = await slaveConnection.execute("CREATE DATABASE IF NOT EXISTS `" + process.env.tgt_db + "`");

  if (targetDbExist) {
    await slaveConnection.end();

    const newSlaveConnection = await mysql.createConnection({
      host: process.env.tgt_host,
      port: process.env.tgt_port,
      user: process.env.tgt_user,
      password: process.env.tgt_password,
      database: process.env.tgt_db,
      authPlugins: {
        mysql_clear_password: () => () => Buffer.from(process.env.tgt_password),
      },
    });

    const [rows] = await newSlaveConnection.execute("SHOW SLAVE STATUS");

    // Stop the replication channel applier thread on the slave server
    await newSlaveConnection.execute("STOP SLAVE SQL_THREAD");

    // Configure replication on the master server
    await masterConnection.execute(`SET GLOBAL binlog_format = 'ROW';`);
    await masterConnection.execute(`SET GLOBAL log_slave_updates = ON;`);
    await createReplUser(masterConnection);
    await createReplUser(newSlaveConnection);
    const [masterStatus] = await masterConnection.execute(`SHOW MASTER STATUS`);
    const { File, Position } = masterStatus[0];

    // Configure replication on the slave server
    if (rows.length <= 0) {
      const replicationChannelName = rows[0]["Channel_Name"];
      await newSlaveConnection.execute(`STOP STOP SLAVE IO_THREAD FOR '${replicationChannelName}'`);
      await newSlaveConnection.execute(
        `CHANGE MASTER TO MASTER_HOST='${process.env.src_host}', MASTER_USER='${newUser}', MASTER_PASSWORD='${newPassword}', MASTER_LOG_FILE='${File}', MASTER_LOG_POS=${Position}`
      );
      await newSlaveConnection.execute("CHANGE MASTER TO GET_MASTER_PUBLIC_KEY=1;");
    }
    await newSlaveConnection.execute(`START SLAVE`);

    // Use pt-table-sync to synchronize data
    exec(
      `perl pt-table-sync-local.pl --execute --no-check-slave --verbose --no-unique-checks --sync-to-slave --databases ${process.env.src_db} h=${process.env.src_host},P=${process.env.src_port},u=${newUser},p=${newPassword} --sync-to-master --databases ${process.env.tgt_db} h=${process.env.tgt_host},P=${process.env.tgt_port},u=${newUser},p=${newPassword}`,
      (error, stdout, stderr) => {
        if (error) {
          console.error(`pt-table-sync error: ${error.message}`);
          return;
        }
        if (stderr) {
          console.error(`pt-table-sync stderr: ${stderr}`);
          return;
        }

        console.log(`pt-table-sync stdout: ${stdout}`);
      }
    );
  }
})();

async function createReplUser(connection) {
  const [rows, fields] = await connection.execute(
    "SELECT EXISTS(SELECT 1 FROM mysql.user WHERE user = '" + newUser + "') AS user_exists"
  );
  const userExists = rows[0].user_exists;

  // Create the user if it does not exist
  if (!userExists) {
    await connection.execute(
      "CREATE USER '" + newUser + "'@'%' IDENTIFIED WITH caching_sha2_password BY '" + newPassword + "'"
    );
  }
  await connection.execute(`ALTER USER '${newUser}'@'%' REQUIRE NONE;`);
  await connection.execute(`GRANT REPLICATION SLAVE ON *.* TO '${newUser}'@'%';`);
  await connection.execute(`GRANT ALL PRIVILEGES ON *.* TO '${newUser}'@'%';`);
  await connection.execute(`FLUSH PRIVILEGES;`);
}
