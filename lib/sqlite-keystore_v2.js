var sqlite = require('sqlite3');

var encoders = {JSON: JSON.stringify};
try {
  encoders.BSON = require('bson');
} catch(e) {}

var queueItem = function(collection, args) {
  this.collection = collection;
  this.args = args;
}

var createCollection = function(collection, callback) {
  //callback for errors
  var self = this;
  var wasqueued = self. _queue.mustQueue;
  self. _queue.mustQueue = true;
  var bindings = {
    $col: collection
  };
  this.db.run("CREATE TABLE IF NOT EXISTS $col (_key INTEGER PRIMARY KEY ASC, encodedData TEXT, encoder TEXT DEFAULT 'JSON');".replace("$col", bindings.$col), function(err) {
    if(!wasqueued) self. _queue.mustQueue = false;
    //process queue.
    if(callback) callback(err);
    setImmediate(self.processQueue.bind(self));
  });
  /*prepped.run(bindings, function(err) {
    prepped.finalize();
    if(!wasqueued) self. _queue.mustQueue = false;
    //process queue.
    if(callback) callback(err);
    process.nextTick(self.processQueue.bind(self));
  });*/
};

var sk = module.exports = function(file, opts) {
  //todo: test file?
  opts = opts || {};
  opts.collection = opts.collection || "_root_";
  opts.mode = opts.mode || (sqlite.OPEN_READWRITE | sqlite.OPEN_CREATE);
  
  this._collection = opts.collection;
  var self = this;

  if(typeof file == 'string') {
    this.db = new sqlite.Database(file, opts.mode, function(err) {
      //populate _root_
      if(err) {
        self.emit('error', {innerErr: err});
        return;
      }
      self.db.run("PRAGMA journal_mode=MEMORY;");
      createCollection.call(self, self._collection, function(err) {
        if(err) {
          self.emit('initialzed', err);
          return;
        }
        self.listIndex(function(err, ind) {
          self.emit('initialzed', err);
        });
      });
    });
  }
  Object.defineProperty(this, "_queue", {
    enumerable: false
    ,writable: true
    ,value: {id: null, mustQueue: false, processing: false, addIndex: [], insert: [], update: [], remove: [], find: []}
  });
  this.__defineGetter__("canProcess", (function(){
    return !this. _queue.mustQueue && !this._queue.processing
  }).bind(self));

  this.indexes = [];
  this.encoder = 'JSON';
};

require('util').inherits(sk, require('events').EventEmitter);

sk.prototype.collection = function(collection) {
  var r = new sk();
  r.db = this.db;
  r._collection = collection;
  createCollection.call(r, r._collection);
  //if collection doesn't exist, create it.

  return r;
};

sk.prototype.listIndex = function(callback) {
  //if(this._collection == '_root_') return [];
  //if(this.indexes.count > 0) return this.indexes;
  var self = this;
  if(typeof callback =='function') self.once('error', callback);
  var wasqueued = this. _queue.mustQueue;
  this. _queue.mustQueue = true;
  var prep = this.db.prepare("SELECT sql FROM sqlite_master WHERE type = 'table' AND tbl_name = $col");
  //console.log("SELECT sql FROM sqlite_master WHERE type = 'table' AND tbl_name = $col");
  prep.get({$col: this._collection}, function(err, t) {
    if(err) {
      //console.log("listIndex:", err)
      if(!wasqueued) self. _queue.mustQueue = false;
      if(typeof callback == 'function')
        callback(err, t);
      else
        self.emit('error', {innerErr: err});
      return;
    }
    self.indexes = [];
      //if((/indf_[_a-z0-9\.]+/i).test(i)) self.indexes.push(i.replace(/^indf_([_a-z0-9]+)/i, "$1"));
    
    var rawInd = t.sql.match(/indf_[a-z_][a-z0-9_\.]+/gi);
    for(var i in rawInd) {
      self.indexes.push(rawInd[i].replace(/^indf_/, ""));
    }
    if(!wasqueued) self._queue.mustQueue = false;
    
    if(typeof callback == 'function')
      callback(err, self.indexes);
    //else
     // self.emit('error', null, self.indexes);
  });
  return self;
  /*
    select sql from master where name = $this.collection
  */
};
sk.prototype.addIndex = function(field, callback, force) {
  var self = this;
  force = force || ((typeof callback == 'boolean') ? callback : null) || false;
  if(!force) {
    self._queue.addIndex.push(new queueItem(self, arguments));
    process.nextTick(self.processQueue.bind(self));
    return self; //QUEUED
  }
  //a.match(r)[2].replace(/\.([0-9]+)/ig, "[$1]")
  //console.log(arguments);
  if(self._collection == '_root_') throw "[No indexes are allowed for _root_.]";
  //if(typeof order == 'function') callback = order;
  //if(typeof order == 'undefined') order = 'ASC';
  var wasqueued = self._queue.mustQueue;
  self._queue.mustQueue = true;
  var cfield = field.replace(/\[([a-z0-9]+)\]/gi, '.$1');
  var indf = 'indf_' + cfield;
  var baseSql = "ALTER TABLE $col ADD column $indf".replace("$col", self._collection).replace("$indf", indf);
  //self.indexes.push(field);
  //console.log(self.db);
  self.indexes.push(cfield)
  self.db.run(baseSql, function(err) {
    if(!wasqueued) self._queue.mustQueue = false;
    if(typeof callback == 'function') callback(err, self.indexes);
  });
  /*
    /\[([a-z_][a-z0-9_]+)\}/gi
    index = 'index_' + field.replace(/\[([a-z0-9]+)\]/gi, '.$1') //replace regex for [].
    indf = 'indf_' + field.replace(/\[([a-z0-9]+)\]/gi, '.$1') //replace regex for [].
    alter table $this.collection add column $index
    create index $index ON $this.collection ($indf $order);

    possibly need to destroy indexes first?
  */
  return self;
};

sk.prototype.processQueue = function(queue) {
  //order: insert, update, remove, find
  var self = this;
  if(queue && !require('util').isArray(queue)) queue = [queue];
  var queues = queue || ['addIndex','insert', 'update', 'remove', 'find'];
  //if(!this.canProcess && !this._queue.processing) {
  if(typeof self._queue.id == 'undefined' || self._queue.id === null) {
    //console.log("waiting to process le queue...", this._queue.mustQueue, this._queue.processing);
    self._queue.id = setImmediate(this.processQueue.bind(this));
    return;
  }

  this._queue.processing = true;
  self.db.serialize(function(){
    for(var i = 0; i < queues.length; i++) {
      var f = queues[i];
      //self.db.serialize(function() {
        while(self._queue[f].length > 0) {
          
          var qi = self._queue[f].shift();
          var nargs = [];
          for(var j in qi.args) {
            if(typeof qi.args[j] != 'undefined') nargs.push(qi.args[j]);
          }
          nargs.push(true);
          self[f].apply(qi.collection, nargs);
        }
      //}); end internal serial
    }
  });
  self._queue.processing = false;
  self._queue.id = null;
};

sk.prototype.insert = function(data, callback, force) {
  var self = this;
  force = force || ((typeof callback == 'boolean') ? callback : null) || false;
  if(!force) {
    this._queue.insert.push(new queueItem(this, arguments));
    //process.nextTick(self.processQueue.bind(self));
    setImmediate(self.processQueue.bind(self));
    return self; //QUEUED;
  }

  //console.log("indexes: %s", self.indexes);
  
  //setup what we save
  var d = {}
  var strInputs = ['$encodedData', '$encoder'];
  var strKeys = ['encodedData', 'encoder'];
  var baseSql = "INSERT INTO $col ( :keys: ) VALUES ( :inputs: );".replace("$col", self._collection);
  d.$encoder = data.encoder || self.encoder;
  d.$encodedData = encoders[d.$encoder](data);
  if(typeof d.$encodedData != 'string') throw "[Encoded data must be a string]"; //todo: more descriptive error.

  var vmContext = require('vm').createContext({data: data});
  for(var i in self.indexes) {
    var ikey = '$indf_' + self.indexes[i];
    var skey = 'indf_' + self.indexes[i];
    var iv = require('vm').runInContext("data." + self.indexes[i], vmContext);
    if(typeof iv == 'undefined') iv = "";
    iv = iv.toString();
    d[ikey] = iv;
    strInputs.push(ikey);
    strKeys.push(skey);
  }
  var prep = self.db.prepare(baseSql.replace(':keys:', strKeys.join()).replace(':inputs:', strInputs.join()));
  //console.log(prep.sql)
  prep.run(d, function(err, r){
    var errObj = {innerErr: err};
    if(err) { 
      self.emit('error', errObj);
      if(typeof callback == 'function') callback(errObj);
      return;
    }
    if(typeof callback == 'function') callback(null, data);
    prep.finalize();
  });
  return self;
};
