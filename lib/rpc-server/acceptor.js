//var acceptor = require('./acceptors/mqtt-acceptor');
// var acceptor = require('./acceptors/ws2-acceptor');
var acceptor = require('./acceptors/mqtt3-acceptor');

module.exports.create = function(opts, cb) {
	return acceptor.create(opts, cb);
};