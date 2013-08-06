var testName = "Entry 1";
console.time(testName);
var sk = require('../lib/sqlite-keystore_v2.js');
var d = new sk(__dirname + '/../test.db');
var err_f = function(err) {
  console.log(err);
}
var c = d.collection('tmp');
d.on('error', err_f);
c.on('error', err_f);
//d.db.on('error', err_f);

c.addIndex('test2', function(err, i) {
  console.log("test2 index err:", err);
  console.log("test2 index:", i)
});
c.addIndex('test', function(err, i){
  console.log(c.indexes);
});
//c.db.on('error', function(e) { console.log(e); });
//c.db.on('open', function() { console.log('open!'); });
var ran = 0;

for(var i = 0; i < 100; i++) {
  //console.log("i:", i)
  var nTestName = testName + ": " + i;
  //console.log(nTestName);
  //console.time(nTestName)
  c.insert({test: (new Date()), test2: 2, test3: [i,2,3, (new Date()).toString()]}, (function(err) {
    ran++;
    if(err) console.log(err);
    //console.timeEnd(this._testname);
    if(ran > 99) {
      //console.timeEnd(testName);
      console.log("finished with:", this.i);
    }
  }).bind({_testname: nTestName, i: i}));
}
