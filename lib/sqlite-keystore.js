var sqlite = require('sqlite3');

function whereClause(where, join) {
  var wstr = [], joiner = join || ' AND ', dv = {};
  for(var i in where) {
    if(i == '$or') {
      var r = this.whereClause(where[i], ' OR ')
      wstr.push(r.statement);
      for(var i in r.data) {
        dv[i] = r.data[i];
      }
      continue;
    }
    var kn = '$a' + Date.now(), kna = i, smb = '=', kv = '$b' + Date.now(), kva = where[i];
    
    if(where[i].$gt) {
      smb = '>';
      kva = where[i].$gt;
    } else if(where[i].$ge) {
      smb = '>=';
      kva = where[i].$ge;
    } else if(where[i].$lt) {
      smb = '<';
      kva = where[i].$lt;
    } else if(where[i].$le) {
      smb = '<=';
      kva = where[i].$le;
    } else if(where[i].$ne) {
      smb = '!=';
      kva = where[i].$ne;
    } else if(where[i].$like) {
      smb = 'LIKE';
      kva = where[i].$like;
    }
    dv[kn] = kna;
    dv[kv] = kva;
    wstr.push("(name = {kn} AND val {smb} {kv})".replace('{kn}', kn).replace('{smb}', smb).replace('{kv}', kv));
    
  }
  
  return {statement: '(' + wstr.join(joiner) + ')', data: dv};
}


var kstore = function(file, opts) {
  //db versioning?
  this.db = new sqlite.Database(file);
  this.db.run("CREATE TABLE IF NOT EXISTS keystore (key TEXT, uid TEXT unique, name TEXT, val TEXT)");
  this.db.run("CREATE TABLE IF NOT EXISTS keyindex (key TEXT UNIQUE)");
}

kstore.prototype.insert = function(key, data, opts, callback) {
  var self = this;
  
  opts = opts || {
    overwrite: false
  }
  if(opts.overwrite) {
    self.update({_key: key}, data, opts, callback);
    return;
  }
  var dmeta = {obj: {_key: key}, count: 0, completed: 0, err: {}};
  for(var i in data) {
    dmeta.count++;
  }

  self.db.serialize(function() {
    self.db.run("INSERT INTO keyindex VALUES ($key)", {$key: key}, function(err,r) {
      if(err === null) {
        for(var i in data) {
          var d = i;
          dmeta.obj[i.toString()] = data[i];
          self.db.run("INSERT INTO keystore VALUES ($key, $uid, $name, $val)", 
                {$key: key, $uid: (key.toString() + i.toString()), $name: i.toString(), $val: data[i]}, 
                function(err, r) {
                  if(err) { dmeta.err[d] = err; }
                  dmeta.completed++;
                  if(dmeta.completed >= dmeta.count) {
                    callback(dmeta.err, dmeta.obj);
                  }
                });
        }
      } else {
-        callback(err);
      }
    });
  });
}

kstore.prototype.update = function(where, data, opts, callback) {
  var self = this;
  
  var dmeta = {obj: {}, count: 0, completed: 0, err: {}};
  for(var i in data) {
    dmeta.count++;
  }
  self.db.serialize(function() {
    if(where._key) {
      self.db.run("INSERT INTO keyindex VALUES ($key)", {$key: where._key}, function(err,r) {});
      dmeta.obj._key = where._key;
    }
    
    for(var i in data) {
      var d = i;
      dmeta.obj[i.toString()] = data[i];
      self.db.run("INSERT OR REPLACE INTO keystore VALUES ($key, $uid, $name, $val)",
              {$key: where._key, $uid: (where._key.toString() + d.toString()), $name: d.toString(), $val: data[d]}, 
              (function(err, r) {
                dmeta.err[this.index] = err;
                dmeta.completed++;
                if(dmeta.completed >= dmeta.count) {
                  callback(dmeta.err, dmeta.obj);
                }
              }).bind({index: d}));
    }
  });
}

kstore.prototype.remove = function(where, opts, callback) {
  if(where._key) {
    var dmeta = {count: 2, completed: 0, err: {}};
    this.db.run("DELETE FROM keyindex WHERE key = $key", {$key: where._key});
    this.db.run("DELETE FROM keystore WHERE key = $key", {$key: where._key});
  }
}

kstore.prototype.select = function(where, opts, callback) {
  var self = this;
  if(where._key) {
    self.db.all("SELECT * FROM keystore WHERE key = $key", {$key: where._key}, function(err, row) {
      var r = {};
      for(var i in row) {
        r[row[i].name] = row[i].val;
      }
      callback(err, r);
    });
  } else {
    var wc = whereClause(where);
    self.db.all("SELECT * FROM keystore WHERE key IN(SELECT key FROM keystore WHERE {where})".replace('{where}', wc.statement), wc.data, function(err, rows) {
      var result = {};
      rows.forEach(function(v, i) {
        if(result[v.key] === undefined) { result[v.key] = {_key: v.key}; }
        result[v.key][v.name] = v.val;
      });
      callback(err, result);
    });
  }
};

kstore.prototype.whereClause = whereClause;

module.exports = kstore;