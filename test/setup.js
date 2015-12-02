/**
 * New node file
 */

var mysql = require('mysql2');
var merge = require('merge');
var assert = require('assert');
var expect = require('chai').expect;
var testDbName = "mysqlcluster_testDbName"

var config = {
	driver: mysql,
	cluster: {
		canRetry: true,
		removeNodeErrorCount: 5,
		defaultSelector: 'RR'
	},
	global: {
		host: 'localhost',
		user: 'root',
		password: '',
		database: testDbName,
		connectionLimit: 100,
		waitForConnections: true,
		queueLimit: 10
	},
	pools: {
		master: {
			config: {
				user: 'write'
			},
			nodes: [{
				host: 'localhost'
			}]
		},
		slave: {
			config: {
				user: 'readonly',
				health: function(poolObject) {
						poolObject.health = {};
						poolObject.health.initialize = setInterval(function() {
							var pool = this.pool;
							pool.getConnection(function(err, conn) {
								conn.query('select (FLOOR(1 + RAND() * 100)) as number', function(err, res) {
									conn.release();
									if(res[0].number % 2 == 0) {
										poolObject.paused = true;
									} else {
										poolObject.paused = false;
									}
								});
							})
						}.bind({
							pool: poolObject.pool
						}), 100);

						poolObject.health.shutdown = function(cb) {
							clearInterval(this.scope);
							cb();
						}.bind({
							scope: poolObject.health.shutdown
						})
					}
			},
			nodes: [{
				host: 'localhost'
			}, {
				host: 'localhost'
			}]
		}
	}
}

describe("MySQL Cluster", function() {
	var serviceConn;

	before(function(done) {
		serviceConn = mysql.createConnection({
			host: 'localhost',
			user: 'root',
			password: ''
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
			if (!err) {
				done();
			} else {
				done(err);
			}
		});
	});

	var cluster;
	it('should instanciate a cluster defined by config', function(done) {
		cluster = require("../lib/index.js")(config);
		expect(cluster).to.be.an('object');
		done();
	});

	it('It should give a connection', function(done) {
		cluster.master(function(err, conn) {
			if(err) {
				done(err);
			} else {
				setTimeout(function() {
					conn.query('select 1', function(err, res) {
						conn.release();
						cluster.end(function() {
							done();
						});
					})
				}, 1500);
			}
		})
	});
});
