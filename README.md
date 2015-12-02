mysql-cluster
=============

`npm install mysql-cluster`

A wrapper around mysql standard driver. Supports a PoolCluster setup with easy master/slave switching.

```
var mysql = require('mysql2');

// Sample config
var config = {
	driver: mysql,		// Optional parameter. You can use a version of mysql or mysql2. Default mysql2 will be used when omitted.
	cluster : {
		canRetry : true,
		removeNodeErrorCount : 5,
		defaultSelector : 'RR'
	},
	global : {
		host : 'localhost',
		user : 'root',
		password : '',
		database : testDbName,
		connectionLimit : 100,
		waitForConnections : true,
		queueLimit : 10
	},
	pools : {
		master : {
			config : {
				user : 'root'
			},
			nodes : [ {
				host : 'localhost'
			} ]
		},
		slave : {
			config : {
				user : 'root'
			},
			nodes : [ {
				host : 'localhost'
			}, {
				host : 'localhost',
				user : 'root'
			} ]
		}
	}
}

var cluster = require("mysql-cluster")(config);

// Aquire connection to one of nodes inside "slave".
cluster.slave(function(err, conn) {
	if(!err) {
		conn.query('select * from test limit 10', function(err, result, fields) {
			// do something
			conn.release(); // Put the connection back into pool.
		}
	} else {
		console.log('Error, failed to aquire connection);
		console.log(err);
	}
}
```
