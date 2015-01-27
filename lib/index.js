
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
			this.__cluster.of(poolname + '*').getConnection(callback);
		}
	};
	
	return new wrapper();
};