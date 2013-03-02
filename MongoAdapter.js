var async = require('async');
var _ = require('underscore');
_.str = require('underscore.string');
var mongodb = require('mongodb');
var mongoClient = mongodb.MongoClient;

module.exports = (function() {

  // Keep track of all the dbs used by the app
  var dbs = {};

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
      }

      return cb();
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
        collection.insert(data, function(err, result) {
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
      spawnConnection(function(connection, cb) {
        var collection = connection.collection(collectionName);
        collection.find.apply(collection, parseFindOptions(options)).toArray(function(err, docs) {
          cb(err, docs);
        });
      }, dbs[collectionName], cb);
    },

    update: function(collectionName, options, values, cb) {
      spawnConnection(function(connection, cb) {
        var collection = connection.collection(collectionName);
        collection.update(options, values, function(err, result) {
          cb(err, result);
        });
      }, dbs[collectionName], cb);
    },

    destroy: function(collectionName, options, cb) {
      spawnConnection(function(connection, cb) {
        var collection = connection.collection(collectionName);
        collection.remove(options, function(err, result) {
          cb(err, result);
        });
      }, dbs[collectionName], cb);
    },

    identity: 'sails-mongo'
  };

  function spawnConnection(logic, config, cb) {
    mongoClient.connect(config.url, function(err, db) {
      afterwards(err, db);
    });

    function afterwards(err, db) {
      if (err) return cb(err);
      logic(db, function(err, result) {
        db.close();
        cb && cb(err, result);
      });
    }
  };

  // Convert standard adapter config
  // into a custom configuration object

  function marshalConfig(config) {
    return _.extend(config, {
      url : config.url
    });
  }

  function parseFindOptions(options) {
    return [options.where, _.omit(options, 'where')];
  }

  return adapter;
})();
