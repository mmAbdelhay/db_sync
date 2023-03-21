const { exec } = require("child_process");

// Run pt-table-checksum command
const checksumCommand = `pt-table-checksum --host=${process.env.src_host} --user=${process.env.src_user} --password=${process.env.src_password} --databases ${process.env.src_db} --tables ${table} --replicate ${process.env.src_db}.checksums --chunk-size=1000 --create-replicate-table --empty-replicate-table --no-check-binlog-format`;

exec(checksumCommand, (err, stdout, stderr) => {
  if (err) {
    console.error(`Error running pt-table-checksum: ${err}`);
    return;
  }

  console.log(`pt-table-checksum output: ${stdout}`);

  // Run pt-table-sync command
  const syncCommand = `pt-table-sync --host=${process.env.tgt_host} --user=${process.env.tgt_user} --password=${process.env.tgt_password} --replicate ${process.env.tgt_db}.checksums --sync-to-master h=${process.env.src_host},P=${process.env.src_port},u=${process.env.src_user},p=${process.env.src_password},D=${process.env.src_db},t=${table} h=${process.env.tgt_host},P=${process.env.tgt_port},u=${process.env.tgt_user},p=${process.env.tgt_password},D=${process.env.tgt_db},t=${table}`;

  exec(syncCommand, (err, stdout, stderr) => {
    if (err) {
      console.error(`Error running pt-table-sync: ${err}`);
      return;
    }

    console.log(`pt-table-sync output: ${stdout}`);
  });
});
