'use strict';

module.exports = LogApplier;

var async = require('async');
var inherits = require('util').inherits;
var EventEmitter = require('events').EventEmitter;

function LogApplier(log, node, persistence) {
  var self = this;
  EventEmitter.call(this);
  this.setMaxListeners(Infinity);
  this.log = log;
  this.node = node;
  this.persistence = persistence;
  this.persisting = false;

  this.queue = async.queue(persist, 1);

  function persist(cb, done) {
    self._persist.call(self, callback);

    function callback() {
      if (cb) {
        cb.apply(null, arguments);
      }
      setImmediate(done);
    }
  }

  this.persist();
}

inherits(LogApplier, EventEmitter);

var LA = LogApplier.prototype;

LA.persist = function persist(cb) {
  this.queue.push(cb);
};

LA._persist = function _persist(cb) {
  var self = this;
  var state = self.node.commonState.volatile;
  var toApply = state.lastApplied + 1;

  if (!this.persisting) {
    this.persisting = true;
    if (state.commitIndex > state.lastApplied) {
      var entry = self.node.commonState.persisted.log.entryAt(toApply);
      if (!entry) {
        done();
      }
      else if (entry.topologyChange) {
        // this is an internal command, a topology command
        // that was already processed (topology commands are processed
        // once they are inserted into the log):
        // we do not need send it to the persistence layer.
        self.persistence.saveCommitIndex(self.node.id, toApply, function(err) {
          if (err) {
            done(err);
          }
          else {
            self.node.save(persisted);
          }
        });
      }
      else {
        self.persistence.applyCommand(
          self.node.id, toApply, entry.command, persisted);
      }
    } else {
      this.persisting = false;
      self.emit('done persisting', state.lastApplied);
      done();
    }
  }

  function done(err) {
    self.persisting = false;
    if (err && !cb) {
      self.emit('error', err);
    }
    if (cb) {
      cb(err);
    }
  }

  function persisted(err) {
    if (err) {
      done(err);
    }
    else {
      self.persisting = false;
      state.lastApplied = toApply;
      self.emit('applied log', toApply);
      self._persist(cb);
    }
  }
};
