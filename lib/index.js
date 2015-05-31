
'use strict';

var mysql = require('mysql2');
var merge = require('merge');

module.exports = function(config) {

	function wrapper() {
		this.__cluster = mysql.createPoolCluster(config.cluster);
		for(var pool in config.pools) {
			if(pool == "__cluster") {
				throw "__cluster is reserved and cannot be used";
			}
			for(var i = 0; i < config.pools[pool].nodes.length; i++) {
				var c = merge(true, config.global, config.pools[pool].config, config.pools[pool].nodes[i]);
				this.__cluster.add(pool + i, c);
			}
			if(!this[pool]) {
				this[pool] = function(callback) {
					this.scope.get(this.pool, callback);
				}.bind({scope: this, pool: pool});
			}
		}

		this.get = function(poolname, callback) {
			this.__cluster.of(poolname + '*').getConnection(function(err, conn) {
				if(err) {
					callback(err, conn);
				} else {
					conn.insert = function(table, data, cb) {
						var fields = Object.keys(data);
						var values = [];
						var dataArray = [table];
						for(var f = 0; f < fields.length; f++) {
							dataArray.push(fields[f]);
						}
						var fieldPh = [];
						var valuePh = [];
						fields.forEach(function(field) {
							fieldPh.push('??');
							valuePh.push('?')
							dataArray.push(data[field]);
						});
						this.query('INSERT INTO ?? (' + fieldPh.join(', ') + ') values (' + valuePh.join(', ')+ ')', dataArray, cb);
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

		this.end = function(cb) {
			this.__cluster(cb);
		}
	};

	return new wrapper();
};
