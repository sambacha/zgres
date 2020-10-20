const pg = require('pg')

var pgConString = process.env.DATABASE_URL

// Connect to the DB
pg.connect(pgConString, function (err, client) {
  if (err) {
    console.error(err)
  }
  // Handle notifications
  client.on('notification', function (msg) {
    const payload = msg.payload
    console.log(payload)
    // Send payload into a queue etc...
  })
  // Listen for NOTIFY calls
  var query = client.query('LISTEN db_notifications')
})
