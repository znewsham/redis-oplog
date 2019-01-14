import { Mongo } from 'meteor/mongo';
import Future from "fibers/future";

let mongoCollection;
let mongoListener;
let mongoPusher;

function getMongoCollection(Config) {
  if (!mongoCollection) {
    mongoCollection = new Mongo.Collection(Config.mongo.collection, {
      _driver: new MongoInternals.RemoteCollectionDriver(Config.mongo.url)
    });
  }
  return mongoCollection;
}

function getRedisCompatibleObject(Config) {
  const collection = getMongoCollection(Config).rawCollection();
  return {
    watchers: {},
    events: {
      message: [],
      error: [],
      connect: [],
      end: [],
      reconnecting: []
    },
    subscribe(channel) {
      if (!this.watchers[channel]) {
        const watcher = collection.watch([{
          $match: {
            operationType: "insert",
            "fullDocument.channel": channel
          }
        }]);
        watcher.on("change", Meteor.bindEnvironment((event) => {
          this.events.message.forEach((handler) => {
            handler(event.fullDocument.channel, event.fullDocument.message);
          });
        }));
        watcher.on("end", console.log);
        watcher.on("close", console.log);
        watcher.on("data", console.log);
        watcher.on("error", Meteor.bindEnvironment((error) => {
          this.events.error.forEach((handler) => {
            handler(error);
          });
        }));
        this.watchers[channel] = watcher;
      }
    },
    unsubscribe(channel) {
      if (this.watchers[channel]) {
        this.watchers[channel].removeAllListeners("error");
        this.watchers[channel].removeAllListeners("change");
        const future = new Future();
        this.watchers[channel].close(() => future.return());
        future.wait();
        delete this.watchers[channel];
      }
    },
    on(event, callback) {
      this.events[event].push(callback);
    },
    publish(channel, message) {
      collection.insert({
        channel,
        message
      });
      //console.log("publish", channel, message);
    }
  };
}

export function getMongoListener(Config) {
  if (!mongoListener) {
    mongoListener = getRedisCompatibleObject(Config);
  }
  return mongoListener;
}

export function getMongoPusher(Config) {
  if (!mongoPusher) {
    mongoPusher = getRedisCompatibleObject(Config);
  }
  return mongoPusher;
}
