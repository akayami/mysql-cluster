'use strict';

//var mysql = require('mysql2');
var merge = require('merge');
var async = require('async');

module.exports = function(config) {


	var mysql = (config.driver ? config.driver : require('mysql2'));

	function wrapper() {

		var poolStack = [];

		var pools = {};
		var pausedNodes = {};

		for (var pool in config.pools) {
			if (!pools[pool]) {
				pools[pool] = {};
				pausedNodes[pool] = {};
			}
			for (var i = 0; i < config.pools[pool].nodes.length; i++) {
				var c = merge(true, config.global, config.pools[pool].config, config.pools[pool].nodes[i]);
				pools[pool][i] = {
					pool: mysql.createPool(c),
					paused: false,
					pause: function() {
						this.paused = true;
						pausedNodes[pool][i] = pools[pool][i];
						delete pools[pool][i];
					},
					unpause: function() {
						this.paused = false;
						pools[pool][i] = pausedNodes[pool][i]
						delete pausedNodes[pool][i];
					}
				};
				if (config.pools[pool].config.health) {
					config.pools[pool].config.health(pools[pool][i]);
				}
			}
			if (!this[pool]) {
				this[pool] = function(callback) {
					this.scope.get(this.pool, callback);
				}.bind({
					scope: this,
					pool: pool
				});
			}
		}

		this.getNode = function(poolname, cb) {

			function pick(obj) {

				function getRandomInt(min, max) {
					return Math.floor(Math.random() * (max - min + 1)) + min;
				}

				var s = Object.keys(obj);
				if (s.length > 0) {
					return getRandomInt(0, s.length - 1);
				} else {
					return null;
				}
			}

			var node = pools[poolname][pick(pools[poolname])];
			if(node != null) {
				cb(null, node);
			} else {
				node = pausedNodes[poolname][pick(pausedNodes[poolname])];
				if(node != null) {
					cb(null, node);
				} else {
					cb(new Error('Failed to find any node for pool ' + poolname));
				}
			}
		}

		this.get = function(poolname, callback) {
			this.getNode(poolname, function(err, node) {
				if (err) {
					callback(err);
				} else {
					node.pool.getConnection(function(err, conn) {
						if (err) {
							callback(err, conn);
						} else {
							conn.insert = function(table, data, cb) {
								var fields = Object.keys(data);
								var values = [];
								var dataArray = [table];
								for (var f = 0; f < fields.length; f++) {
									dataArray.push(fields[f]);
								}
								var fieldPh = [];
								var valuePh = [];
								fields.forEach(function(field) {
									fieldPh.push('??');
									valuePh.push('?')
									dataArray.push(data[field]);
								});
								this.query('INSERT INTO ?? (' + fieldPh.join(', ') + ') values (' + valuePh.join(', ') + ')', dataArray, cb);
							};

							conn.update = function(table, data, condition, cond_params, cb) {
								var fields = Object.keys(data);
								var values = [];
								var dataArray = [table];
								var fieldPh = [];
								fields.forEach(function(field, f) {
									fieldPh.push('??=?');
									dataArray.push(fields[f]);
									dataArray.push(data[field]);
								});
								cond_params.forEach(function(param) {
									dataArray.push(param);
								})
								this.query('UPDATE ?? SET ' + fieldPh.join(', ') + ' WHERE ' + condition, dataArray, cb);
							};
							callback(err, conn);
						}
					});
				}
			});
		}

		this.end = function(cb) {
			var tasks = [];
			for (var name in pools) {
				for (var i in pools[name]) {
					tasks.push(function(callback) {
						var pool = this.pool;
						if (pool.health && pool.health.shutdown) {
							pool.health.shutdown(function() {
								pool.pool.end(function(err) {
									callback(err);
								})
							})
						} else {
							pool.pool.end(function(err) {
								callback(err);
							})
						}
					}.bind({
						pool: pools[name][i]
					}))
				}
			}
			async.parallel(tasks, function(err, results) {
				cb(err);
			});
		}
	};

	return new wrapper();
};
