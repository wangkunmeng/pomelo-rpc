var logger = require('pomelo-logger').getLogger('pomelo-rpc', 'mqtt-acceptor');
var EventEmitter = require('events').EventEmitter;
var Tracer = require('../../util/tracer');
var utils = require('../../util/utils');
var aedes = require('aedes')();

var util = require('util');
var net = require('net');


var Acceptor = function(opts, cb) {
    EventEmitter.call(this);
    this.interval = opts.interval; // flush interval in ms
    this.bufferMsg = opts.bufferMsg;
    this.rpcLogger = opts.rpcLogger;
    this.rpcDebugLog = opts.rpcDebugLog;
    this._interval = null; // interval object
    this.sockets = {};
    this.msgQueues = {};
    this.cb = cb;
};

util.inherits(Acceptor, EventEmitter);

var pro = Acceptor.prototype;

pro.listen = function(port) {
    //check status
    if (!!this.inited) {
        this.cb(new Error('already inited.'));
        return;
    }
    this.inited = true;

    var self = this;
    this.server = net.createServer(aedes.handle);
    this.server.listen(port, function(){

        aedes.on('clientError', function (client, err) {
            logger.error('rpc client error', client.id, err.message, err.stack)
        });

        aedes.on('connectionError', function (client, err) {
            logger.error('rpc client error', client, err.message, err.stack)
        })
        aedes.on('clientReady', function (client) {
            console.log("client has been connected");
            self.sockets[client.id] = client;
        });
        aedes.on('subscribe', function(packet, client){
            console.log("subscribe")
        })
        aedes.on('publish', function (packet, client) {
            console.log("publish"+ packet.payload.toString())
            if (client) {
                pkg = packet.payload.toString();
                console.log(pkg)
                var isArray = false;
                try {
                    pkg = JSON.parse(pkg);
                    if (pkg instanceof Array) {
                        processMsgs(client, self, pkg);
                        isArray = true;
                    } else {
                        processMsg(socket, self, pkg);
                    }
                } catch (err) {
                    if (!isArray) {
                        doSend(client, {
                            id: pkg.id,
                            resp: [cloneError(err)]
                        });
                    }
                    logger.error('process rpc message error %s', err.stack);
                }
            }
        })
        aedes.on("clientDisconnect", function (client) {
            self.onSocketClose(client);
        })
    });




    /*if (this.bufferMsg) {
        this._interval = setInterval(function () {
            flush(self);
        }, this.interval);
    }*/
}


pro.close = function() {
    if (this.closed) {
        return;
    }
    if (this._interval) {
        clearInterval(this._interval);
        this._interval = null;
    }
    this.server.close();
    this.emit('closed');
};

pro.onSocketClose = function(socket) {
        var id = socket.id;
        delete this.sockets[id];
        delete this.msgQueues[id];
}

var cloneError = function(origin) {
    // copy the stack infos for Error instance json result is empty
    var res = {
        msg: origin.msg,
        stack: origin.stack
    };
    return res;
};

var processMsg = function(socket, acceptor, pkg) {
    var tracer = null;
    if (this.rpcDebugLog) {
        tracer = new Tracer(acceptor.rpcLogger, acceptor.rpcDebugLog, pkg.remote, pkg.source, pkg.msg, pkg.traceId, pkg.seqId);
        tracer.info('server', __filename, 'processMsg', 'mqtt-acceptor receive message and try to process message');
    }
    acceptor.cb(tracer, pkg.msg, function() {
        // var args = Array.prototype.slice.call(arguments, 0);
        var len = arguments.length;
        var args = new Array(len);
        for (var i = 0; i < len; i++) {
            args[i] = arguments[i];
        }

        var errorArg = args[0]; // first callback argument can be error object, the others are message
        if (errorArg && errorArg instanceof Error) {
            args[0] = cloneError(errorArg);
        }

        var resp;
        if (tracer && tracer.isEnabled) {
            resp = {
                traceId: tracer.id,
                seqId: tracer.seq,
                source: tracer.source,
                id: pkg.id,
                resp: args
            };
        } else {
            resp = {
                id: pkg.id,
                resp: args
            };
        }
        if (acceptor.bufferMsg) {
            enqueue(socket, acceptor, resp);
        } else {
            doSend(socket, resp);
        }
    });
};

var processMsgs = function(socket, acceptor, pkgs) {
    for (var i = 0, l = pkgs.length; i < l; i++) {
        processMsg(socket, acceptor, pkgs[i]);
    }
};

var enqueue = function(socket, acceptor, msg) {
    var id = socket.id;
    var queue = acceptor.msgQueues[id];
    if (!queue) {
        queue = acceptor.msgQueues[id] = [];
    }
    queue.push(msg);
};

var flush = function(acceptor) {
    var sockets = acceptor.sockets,
        queues = acceptor.msgQueues,
        queue, socket;
    for (var socketId in queues) {
        socket = sockets[socketId];
        if (!socket) {
            // clear pending messages if the socket not exist any more
            delete queues[socketId];
            continue;
        }
        queue = queues[socketId];
        if (!queue.length) {
            continue;
        }
        doSend(socket, queue);
        queues[socketId] = [];
    }
};

var doSend = function(socket, msg) {
    socket.publish({
        topic: 'rpc',
        payload: JSON.stringify(msg)
    });
}

/**
 * create acceptor
 *
 * @param opts init params
 * @param cb(tracer, msg, cb) callback function that would be invoked when new message arrives
 */
module.exports.create = function(opts, cb) {
    return new Acceptor(opts || {}, cb);
};