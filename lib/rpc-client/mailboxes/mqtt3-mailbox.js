var logger = require('pomelo-logger').getLogger('pomelo-rpc', 'mqtt2-mailbox');
var EventEmitter = require('events').EventEmitter;
var Constants = require('../../util/constants');
var Tracer = require('../../util/tracer');
//var MqttCon = require('mqtt-connection');
var utils = require('../../util/utils');
var Coder = require('../../util/coder');
var util = require('util');
var net = require('net');
var mqtt = require('mqtt');

var CONNECT_TIMEOUT = 3000;

var MailBox = function(server, opts) {
    EventEmitter.call(this);
    this.curId = 0;
    this.id = server.id;
    this.host = server.host;
    this.port = server.port;
    this.requests = {};
    //this.timeout = {};
    this.queue = [];
    this.servicesMap = {};
    this.bufferMsg = opts.bufferMsg;
    this.keepalive = opts.keepalive || Constants.DEFAULT_PARAM.KEEPALIVE;
    this.interval = opts.interval || Constants.DEFAULT_PARAM.INTERVAL;
    this.timeoutValue = opts.timeout || Constants.DEFAULT_PARAM.CALLBACK_TIMEOUT;
    this.lastPing = -1;
    this.lastPong = -1;
    this.connected = false;
    this.closed = false;
    this.opts = opts;
    this.serverId = opts.context.serverId;
};

util.inherits(MailBox, EventEmitter);

MailBox.prototype.connect = function(tracer, cb) {
    tracer && tracer.info('client', __filename, 'connect', 'mqtt-mailbox try to connect');
    if (this.connected) {
        tracer && tracer.error('client', __filename, 'connect', 'mailbox has already connected');
        return cb(new Error('mailbox has already connected.'));
    }

    var self = this;


    var port = this.port;
    var host = this.host;
    this.socket = mqtt.Client(function(){
        var stream = net.createConnection(port, host);
        stream.setNoDelay(true);
        return stream;
    }, {
        protocolVersion:"3.1.1"
    });
    this.socket.on("connect", function(){
        if(self.connected) {
            return;
        }
        self.connected = true;
        if (self.bufferMsg) {
            self._interval = setInterval(function() {
                flush(self);
            }, self.interval);
        }
    });

    this.socket.on("message", function(topic, message, pkg){
        if(pkg.topic == Constants['TOPIC_HANDSHAKE']) {
            upgradeHandshake(self, pkg.payload);
            return cb();
        }
        try {
            pkg = Coder.decodeClient(pkg.payload);
            processMsg(self, pkg);
        } catch (err) {
            logger.error('rpc client %s process remote server %s message with error: %s', self.serverId, self.id, err.stack);
        }
    })

    this.socket.on('error', function(err) {
        logger.error('rpc socket %s is error, remote server %s host: %s, port: %s', self.serverId, self.id, self.host, self.port);
        self.emit('close', self.id);
        self.close();
    });


    this.socket.on('disconnect', function(reason) {
        logger.error('rpc socket %s is disconnect from remote server %s, reason: %s', self.serverId, self.id, reason);
        var reqs = self.requests;
        for (var id in reqs) {
            var ReqCb = reqs[id];
            ReqCb(tracer, new Error(self.serverId + ' disconnect with remote server ' + self.id));
        }
        self.emit('close', self.id);
    });


    var connectTimeout = setTimeout(function() {
        logger.error('rpc client %s connect to remote server %s timeout', self.serverId, self.id);
        self.emit('close', self.id);
    }, CONNECT_TIMEOUT);



    this.socket.on('publish', function(pkg) {
        if(pkg.topic == Constants['TOPIC_HANDSHAKE']) {
            upgradeHandshake(self, pkg.payload);
            return cb();
        }
        try {
            pkg = Coder.decodeClient(pkg.payload);
            processMsg(self, pkg);
        } catch (err) {
            logger.error('rpc client %s process remote server %s message with error: %s', self.serverId, self.id, err.stack);
        }
    });

    this.socket.on('error', function(err) {
        logger.error('rpc socket %s is error, remote server %s host: %s, port: %s', self.serverId, self.id, self.host, self.port);
        self.emit('close', self.id);
        self.close();
    });



    this.socket.on('disconnect', function(reason) {
        logger.error('rpc socket %s is disconnect from remote server %s, reason: %s', self.serverId, self.id, reason);
        var reqs = self.requests;
        for (var id in reqs) {
            var ReqCb = reqs[id];
            ReqCb(tracer, new Error(self.serverId + ' disconnect with remote server ' + self.id));
        }
        self.emit('close', self.id);
    });
};

/**
 * close mailbox
 */
MailBox.prototype.close = function() {
    this.closed = true;
    this.connected = false;
    if(this.socket) {
        this.socket.close();
    }
};

/**
 * send message to remote server
 *
 * @param msg {service:"", method:"", args:[]}
 * @param opts {} attach info to send method
 * @param cb declaration decided by remote interface
 */
MailBox.prototype.send = function(tracer, msg, opts, cb) {
    tracer && tracer.info('client', __filename, 'send', 'mqtt-mailbox try to send');
    if (!this.connected) {
        tracer && tracer.error('client', __filename, 'send', 'mqtt-mailbox not init');
        cb(tracer, new Error(this.serverId + ' mqtt-mailbox is not init ' + this.id));
        return;
    }

    if (this.closed) {
        tracer && tracer.error('client', __filename, 'send', 'mailbox has already closed');
        cb(tracer, new Error(this.serverId + ' mqtt-mailbox has already closed ' + this.id));
        return;
    }

    var id = this.curId++;
    this.requests[id] = cb;

    var pkg;
    if (tracer && tracer.isEnabled) {
        pkg = {
            traceId: tracer.id,
            seqId: tracer.seq,
            source: tracer.source,
            remote: tracer.remote,
            id: id,
            msg: msg
        };
    } else {
        pkg = Coder.encodeClient(id, msg, this.servicesMap);
        // pkg = {
        //   id: id,
        //   msg: msg
        // };
    }
    if (this.bufferMsg) {
        enqueue(this, pkg);
    } else {
        doSend(this.socket, pkg, tracer, cb);
    }
};



var enqueue = function(mailbox, msg) {
    mailbox.queue.push(msg);
};

var flush = function(mailbox) {
    if (mailbox.closed || !mailbox.queue.length) {
        return;
    }
    doSend(mailbox.socket, mailbox.queue);
    mailbox.queue = [];
};

var doSend = function(socket, msg, mailbox, id, tracer, cb) {
    socket.publish('rpc', JSON.stringify(msg), function(err){
        if(err) {
            if(cb){
                if (mailbox.requests[id]) {
                    delete mailbox.requests[id];
                }
                cb(tracer, new Error(err))
            }
        }
    });
}

var upgradeHandshake = function(mailbox, msg) {
    var servicesMap = JSON.parse(msg.toString());
    mailbox.servicesMap = servicesMap;
}

var processMsgs = function(mailbox, pkgs) {
    for (var i = 0, l = pkgs.length; i < l; i++) {
        processMsg(mailbox, pkgs[i]);
    }
};

var processMsg = function(mailbox, pkg) {
    var pkgId = pkg.id;
    var cb = mailbox.requests[pkgId];
    if (!cb) {
        return;
    }
    delete mailbox.requests[pkgId];
    var rpcDebugLog = mailbox.opts.rpcDebugLog;
    var tracer = null;
    var sendErr = null;
    if (rpcDebugLog) {
        tracer = new Tracer(mailbox.opts.rpcLogger, mailbox.opts.rpcDebugLog, mailbox.opts.clientId, pkg.source, pkg.resp, pkg.traceId, pkg.seqId);
    }
    var pkgResp = pkg.resp;
    cb(tracer, sendErr, pkgResp);
};

/**
 * Factory method to create mailbox
 *
 * @param {Object} server remote server info {id:"", host:"", port:""}
 * @param {Object} opts construct parameters
 *                      opts.bufferMsg {Boolean} msg should be buffered or send immediately.
 *                      opts.interval {Boolean} msg queue flush interval if bufferMsg is true. default is 50 ms
 */
module.exports.create = function(server, opts) {
    return new MailBox(server, opts || {});
};