/**
 * New node file
 */

var mysql = require('mysql');
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
	var connection;
	it("should be able to aquire master connection using master()", function(done) {
		cluster.master(function(err, conn) {
			if(err) {
				done(err);
			} else {
				connection = conn;
				done();
			}
		});
	});
	
	it('should be able to create a table', function(done) {
		connection.query('CREATE TABLE ?? (`id` INT UNSIGNED NOT NULL AUTO_INCREMENT,`test` VARCHAR(45) NULL, PRIMARY KEY (`id`));', ['test'], function(err, result) {
			if(err) {
				done(err);
			} else {
				done();
			}
		});
	});
	
	it('should be able to write into table', function(done) {
		connection.query('INSERT INTO ?? SET `test`=?', ['test', 'hello'], function(err, result) {
			if(err) {
				done(err);
			} else {
				done();
			}
		});
	});
	it('should be able to read from table', function(done) {
		connection.query('select * from ?? where id=1', ['test'], function(err, result) {
			if(err) {
				done(err);
			} else {
				done();
			}
		});
	});
	it('should be able to drop a table', function(done) {
		connection.query('drop table ??', ['test'], function(err, result) {
			if(err) {
				done(err);
			} else {
				done();
			}
		});
	});
	
	it('should not be able to read from a droped table', function(done) {
		connection.query('select * from ?? where id=1', ['test'], function(err, result) {
			if(err) {
				done();
			} else {
				done(new Error('Was able to read from table that should not have existed !'));
			}
		});
	});
});
