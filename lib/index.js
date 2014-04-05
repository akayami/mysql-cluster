
'use strict';

var mysql = require('mysql');
var merge = require('merge');

module.exports = function(config) {
	
	function wrapper() {
		this.__cluster = mysql.createPoolCluster(config.cluster);
		for(var pool in config.pools) {
			if(pool == "__cluster") {
				throw "__cluster is reserved and cannot be used";
			}
			for(var i = 0; i < config.pools[pool].nodes.length; i++) {
				var c = merge(config.global, config.pools[pool].config, config.pools[pool].nodes[i]);	
				this.__cluster.add(pool + i, c);
			}
			this[pool] = function(callback) {
				this.get(pool, callback);
			} 
		}
		
		this.get = function(poolname, callback) {
			console.log(poolname);
			this.__cluster.of(poolname + '*').getConnection(callback);
		}
	};
	
	return new wrapper();
};