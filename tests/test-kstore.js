var assert = require('assert');
var k = require('../');
var fs = require('fs');
try {
  fs.unlinkSync('./test-kstore.sqlite3');
} catch(e) {}
var ks = new k('./test-kstore.sqlite3');

assert.equal(fs.existsSync('./test-kstore.sqlite3'), true);

ks.insert(1, {test: 'abc', abc: 'test'}, null, function(err, d) {
  ks.select({_key: 1}, null, function(err, data) {
    assert.equal(data.test, 'abc');
    
    ks.update({_key: 1}, {test: 'xyz'}, null, function(err, data) {
      ks.select({_key: 1}, null, function(err, data) {
        assert.equal(data.test, 'xyz');
      });
    });
  });
});
ks.insert(1, {test: 'abc', abc: 'test'}, null, function(err, d) {
  assert.notEqual(err, null);
});
ks.update({_key: 2}, {something: 'else'}, null, function(err, data) {
  ks.select({_key: 2}, null, function(err, data) {
    ks.update({_key: 2}, {somethingnew: 'hi'}, null, function(err, data) {
      ks.select({_key: 2}, null, function(err, data) {
        assert.equal(data.something, 'else');
      });
    });
  });
});

var ins = {vals: [1,2,3,4,5,6,7,8,9,10], completed: 0}

ins.vals.forEach(function(v,i) {
  ks.insert(i+100, {data: v, indexer: i}, null, function(err, d) {
    ins.completed++;
    if(ins.completed >= ins.vals.length) {
      console.log(ks.whereClause({data: {$gt: 5}}));
      ks.select({data: {$gt: 5}}, null, function(err, data) {
        console.log(data);
      });
    }
  });
});