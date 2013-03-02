var async = require('async');
var _ = require('underscore');
_.str = require('underscore.string');
var mongodb = require('mongodb');

module.exports = (function() {
  var adapter = {
    syncable: true,

    registerCollection: function(collection, cb) {
      var self = this;

      // If the configuration in this collection corresponds
      // with a known database, reuse it the connection(s) to that db
      dbs[collection.identity] = _.find(dbs, function(db) {
        return collection.host === db.host && collection.database === db.database;
      });

      // Otherwise initialize for the first time
      if (!dbs[collection.identity]) {
        dbs[collection.identity] = marshalConfig(collection);
      } else return cb();
    },

    teardown: function(cb) {
      cb && cb();
    },

    describe: function(collectionName, cb) {
      //It's mongo -- there's nothing to describe
      return cb(null, {});
    },

    define: function(collectionName, definition, cb) {
      spawnConnection(function __DEFINE__(connection, cb) {
        connection.createCollection(collectionName, function __DEFINE__(err, result) {
          if (err) return cb(err);
          cb(null, result);
        });
      }, dbs[collectionName], cb);
    },

    drop: function(collectionName, cb) {
      spawnConnection(function __DROP__(connection, cb) {
        connection.dropCollection(collectionName, function __DEFINE__(err, result) {
          if (err) return cb(err);
          cb(null, result);
        });
      }, dbs[collectionName], cb);
    },

    create: function(collectionName, data, cb) {
      spawnConnection(function(connection, cb) {
        var collection = connection.collection(collectionName);
        colleciton.insert(data, function(err, result) {
          if (err) return cb(err);

          // Build model to return
          var model = _.extend({}, data, {

            // TODO: look up the autoIncrement attribute and increment that instead of assuming `id`
            id: result._id
          });

          cb(err, model);
        });
      }, dbs[collectionName], cb);
    },

    find: function(collectionName, options, cb) {
    },

    update: function(collectionName, options, values, cb) {
    },

    destroy: function(collectionName, options, cb) {
    },

    identity: 'sails-mongo'
  };

  function spawnConnection(logic, config, cb) {
    var marshalledConfig = marshalConfig(config);
    var connection = new mongodb.Db(marshalledConfig.database, new mongodb.Server(marshalledConfig.hostname, marshalledConfig.port, {}), {w: 1});
    connection.open(function(err) {
      afterwards(err, connection);
    });

    function afterwards(err, connection) {
      if (err) return cb(err);
      logic(connection, function(err, result) {
        connection.close();
        cb && cb(err, result);
      });
    }
  };

  // Convert standard adapter config
  // into a custom configuration object

  function marshalConfig(config) {
    return _.extend(config, {
      host     : config.host,
      port     : config.port,
      user     : config.user,
      password : config.password,
      database : config.database
    });
  }
})();
