/**
 * New node file
 */

var mysql = require('mysql2');
var merge = require('merge');
var assert =  require('assert');
var expect = require('chai').expect;
var testDbName = "mysqlcluster_testDbName"

var config = {
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
				user : 'write'
			},
			nodes : [ {
				host : 'localhost'
			} ]
		},
		slave : {
			config : {
				user : 'readonly'
			},
			nodes : [ {
				host : 'localhost'
			}, {
				host : 'localhost'
			} ]
		}
	}
}

//console.log(config.pools.master.config);
//console.log(config.pools.slave);

var mysql = require('mysql');

describe("MySQL Cluster", function() {
	var serviceConn;

	before(function(done) {
		serviceConn = mysql.createConnection({
			host : 'localhost',
			user : 'root',
			password : ''
		});
		serviceConn.connect(function(err) {
			if (err) {
				done(err);
			} else {
				done();
			}
		})
	});

	before(function(done) {
		serviceConn.query('create database ??', [testDbName], function(err, result) {
			if (!err) {
				done();
			} else {
				done(err);
			}
		});
	});

	before(function(done) {
		serviceConn.query('use ??', [testDbName], function(err, result) {
			if (!err) {
				done();
			} else {
				done(err);
			}
		});
	});

	before(function(done) {
		serviceConn.query('GRANT ALL ON ??.* TO ?@? IDENTIFIED BY ?', [testDbName, 'write', 'localhost', ''], function(err, result) {
			if (!err) {
				done();
			} else {
				done(err);
			}
		});
	});

	before(function(done) {
		serviceConn.query('GRANT SELECT ON ??.* TO ?@? IDENTIFIED BY ?', [testDbName, 'readonly', 'localhost', ''], function(err, result) {
			if (!err) {
				done();
			} else {
				done(err);
			}
		});
	});

	after(function(done) {
		serviceConn.query('REVOKE ALL ON ??.* FROM ?@localhost', [testDbName, 'write'], function(err, result) {
			if (!err) {
				done();
			} else {
				done(err);
			}
		});
	});

	after(function(done) {
		serviceConn.query('REVOKE SELECT ON ??.* FROM ?@localhost', [testDbName, 'readonly'], function(err, result) {
			if (!err) {
				done();
			} else {
				done(err);
			}
		});
	});

	after(function(done) {
		serviceConn.query('drop database ??', [testDbName], function(err, result) {
			if(!err) {
				done();
			} else {
				done(err);
			}
		});
	});



	var cluster;
	it('should instanciate a cluster defined by config', function() {
		cluster = require("../lib/index.js")(config);
		expect(cluster).to.be.an('object');
	});

	var master;
	it("should be able to aquire master connection using master()", function(done) {
		cluster.master(function(err, conn) {
			if(err) {
				done(err);
			} else {
				master = conn;
				done();
			}
		});
	});


	var slave;
	it("should be able to aquire slave connection using slave()", function(done) {
		cluster.slave(function(err, conn) {
			if(err) {
				done(err);
			} else {
				slave = conn;
				done();
			}
		})
	})

	it('should be able to create a table using master connection', function(done) {
		master.query('CREATE TABLE ?? (`id` INT UNSIGNED NOT NULL AUTO_INCREMENT,`test` VARCHAR(45) NULL, PRIMARY KEY (`id`));', ['test'], function(err, result) {
			if(err) {
				done(err);
			} else {
				done();
			}
		});
	});

	it('should NOT be able to create a table using slave connection', function(done) {
		slave.query('CREATE TABLE ?? (`id` INT UNSIGNED NOT NULL AUTO_INCREMENT,`test` VARCHAR(45) NULL, PRIMARY KEY (`id`));', ['test_slave'], function(err, result) {
			if(err) {
				done();
			} else {
				done(new Error('Was able to create a table using a slave connection'));
			}
		});
	});

	it('should be able to write into table', function(done) {
		master.query('INSERT INTO ?? SET `test`=?', ['test', 'hello'], function(err, result) {
			if(err) {
				done(err);
			} else {
				done();
			}
		});
	});

	it('should be able to write into table using shorcut insert method', function(done) {
		master.insert('test', {test: 'hello'}, function(err, result) {
			if(err) {
				done(err)
			} else {
				if(result.insertId) {
					done();
				}
			}
		});
	});

	it('should NOT be able to write into table usign slave connection', function(done) {
		slave.query('INSERT INTO ?? SET `test`=?', ['test', 'hello'], function(err, result) {
			if(err) {
				done();
			} else {
				done(new Error('Was able to write into a table using slave connection'));
			}
		});
	});

	it('should update a field', function(done) {
		master.update('test', {test: 'test updated'}, 'ID = ?', [1], function(err, result) {
			if(err) {
				done(err);
			} else {
				done();
			}
		})
	});

	it('should be able to read from table using named placeholders', function(done) {
		master.config.namedPlaceholders = true;
		master.execute('select * from test where id = :id', {table: 'test', id: 1}, function(err, result) {
			if(err) {
				done(err);
			} else {
				done();
			}
		});
	});

	it('should be able to read from table', function(done) {
		master.query('select * from ?? where id=1', ['test'], function(err, result) {
			if(err) {
				done(err);
			} else {
				done();
			}
		});
	});
	it('should be able to drop a table', function(done) {
		master.query('drop table ??', ['test'], function(err, result) {
			if(err) {
				done(err);
			} else {
				done();
			}
		});
	});

	it('should not be able to read from a dropped table', function(done) {
		master.query('select * from ?? where id=1', ['test'], function(err, result) {
			if(err) {
				done();
			} else {
				done(new Error('Was able to read from table that should not have existed !'));
			}
		});
	});
});
