/**
 * @fileoverview EventStore implementation with Redis Streams
 * @author Joey Whelan <joey.whelan@gmail.com>
 */

/*jshint esversion: 6 */

'use strict';
'use esversion 6';
const redis = require('redis');
const util = require('util');
const events = require('events');
const logger = require('./eventStoreLogger');


/** @desc EventStore implementation with Redis Streams  */
module.exports = class EventStoreClient {
	constructor(redisPort, redisHost, readInterval = 30000) {
		//internal variables to support event-based subscriptions to the redis streams
		this._emitters = {};
		this._intervals = [];
		this._redisPort = redisPort;
		this._redisHost = redisHost;
		this._readInterval = readInterval;
	}
	
	/**
	 * Implements a Redis ack on an event
	 * @param {string} streamName - name of the Redis stream
	 * @param {string} timestamp - Redis timestamp (id).
	 * @return {integer} - number of events ack'ed.  Should equal 1.
	 */
	ack(streamName, timestamp) {
		let groupName = streamName + 'Group';
		logger.debug(`EventStoreClient.ack - streamName:${streamName}, groupName:${groupName}, timestamp:${timestamp}`);
		return this._xackAsync(streamName, groupName, timestamp)
		.then(result => {
			logger.debug(`EventStoreClient.ack - result:${result}`);
			return result;
		})
		.catch(err => {
			logger.error(`EventStoreClient.ack - streamName:${streamName}, timestamp:${timestamp} - ${err}`);
			throw err;	
		});
	}
	
	/**
	 * Adds an id to a Redis set.  This set is used to keep track of unique ids.
	 * @param {string} id - ID of game to be added
	 * @param {string} type - type of id: accountId
	 * @return {int} - 1 indicates the add was successful (unique), 0 indicates it was not a unique add to the set
	 */
	addId(id, type) {
		return this._saddAsync(type,id)
		.then(result => {
			logger.debug(`EventStoreClient.addId - id:${id} - result:${result}`);
			return result;
		})
		.catch(err => {
			logger.error(`EventStoreClient.addId - id:${id} - ${err}`);
			throw err;	
		});
	}
	
	/**
	 * Function for clean up.  Shuts down redis client.
	 * @return void
	 */
	close() {
		logger.debug(`EventStoreClient.close`);
		this._intervals.forEach((obj) => {
			obj.clearInterval();
		});
		this._client.quit();
	}
	
	/**
	 * Creates a redis client for stream operations
	 * @param {int} redisPort - redis port number
	 * @param {string} redisHost - redis host name/address
	 */
	connect(){
		logger.debug(`EventStoreClient.connect`);
		this._client = redis.createClient(this._redisPort, this._redisHost);
		this._client.on('error', (err) => {
			logger.error(`EventStoreClient - redis client error:${err}`);
			throw err;
		});

		//promisified versions of Redis commands
		this._saddAsync = util.promisify(this._client.sadd).bind(this._client);
		this._xackAsync = util.promisify(this._client.xack).bind(this._client);
		this._xclaimAsync = util.promisify(this._client.xclaim).bind(this._client);
		this._xpendingAsync = util.promisify(this._client.xpending).bind(this._client);
		this._xreadAsync = util.promisify(this._client.xread).bind(this._client);
		this._xreadgroupAsync = util.promisify(this._client.xreadgroup).bind(this._client);
		this._getAsync = util.promisify(this._client.get).bind(this._client);	
	}
	
	/**
	 * Fetches all the events from a given stream, for a given id after the given timestamp
	 * @param {string} streamName - name of the Redis stream
	 * @param {string} id - ID of the events to be returned from the stream
	 * @param {string} timestamp - Redis timestamp.  Provides a 'snapshot' functionality such that only events
	 * that have occurred since the timestamp will be returned.
	 * @return {array} - array of events with the id after the timestamp
	 */
	get(streamName, id, timestamp) {
		return this._xreadAsync('streams', streamName, timestamp)
		.then(results => {
			let eventList = [];
			if (results) {
				let events = results[0][1];
				for (let i=0; i<events.length; i++) {
					let obj = JSON.parse(events[i][1][1]);
					if (obj.id === id) {
						obj.timestamp = events[i][0];
						eventList.push(obj);
					}
				}
			}
			
			logger.debug(`EventStoreClient.get - streamName:${streamName}, id:${id}, timestamp:${timestamp}\
			 - eventList.length:${eventList.length}`);
			return eventList;
		})
		.catch(err => {
			logger.error(`EventStoreClient.get - streamName:${streamName}, id:${id}, timestamp:${timestamp} - ${err}`);
			throw err;
		});
	}
	
	/**
	 * Fetches events from the pending queue of a Redis stream.  If the event has set in queue for a time > maxElapsed,
	 * that event is re-'claimed' by another subscriber for processing.
	 * @param {string} streamName - name of the Redis stream
	 * @param {string} consumerName - name of the subscriber which would re-claim the event for processing
	 * @param {int} maxElapsed - time (in ms) beyond which a queued event would be re-claimed
	 * @return {array} - array of pending events that were in queue for a time > maxElapsed
	 */
	getPending(streamName, consumerName, maxElapsed) {
		logger.debug(`EventStoreClient.getPending - streamName:${streamName}, consumerName:${consumerName}, maxElapsed:${maxElapsed}`);
		let groupName = streamName + 'Group';
		return this._xpendingAsync(streamName, groupName, '-', '+', '-1')  //get the entire list of pending events
		.then((results) => {
			let promises = [];
			results.forEach((result) => {  //put a series of reclaim requests into a promise array
				let timestamp = result[0];
				let elapsed = result[2];
				if (elapsed >= maxElapsed) {	
					promises.push(this._xclaimAsync(streamName, groupName, consumerName, maxElapsed, timestamp));
				}
			});
			return Promise.all(promises);
		})
		.then((results) => {
			let eventList = [];
			results.forEach((result) => {  //parse out the event object and timestamp of the event
				let obj = JSON.parse(result[0][1][1]);
				obj.timestamp = result[0][0];
				eventList.push(obj);
			});
			logger.debug(`EventStoreClient.getPending - eventList.length:${eventList.length}`);
			return eventList;
		})
		.catch(err => {
			if (err.code === 'NOGROUP') {
				return [];
			}
			else {
				logger.error(`EventStoreClient.getPending - streamName:${streamName}, consumerName:${consumerName} - ${err}`);
				throw err;
			}
		});
	}
	
	/**
	 * Publishes an event to a Redis stream object.  Implements optimistic concurrency control by utilizing a Redis key
	 * per id (unique account id).  The add to the Redis stream is wrapped in a Watch on Redis key with the same value
	 * as the id.  The Redis key contains a value representing the current 'version' of the id.  The version is incremented and
	 * the event added to a Redis stream as 1 transaction (atomic) within the Watch command.  If two or more processes
	 * attempt to add an event simultaneously, only 1 will succeed.  
	 * @param {string} streamName - name of the Redis stream
	 * @param {Object} event - object containing the event to be published.  JSON-stringified prior adding to Redis stream.
	 * @return {array} - 2 values in an array, if successful. Null, if not.  Value 1 = new version number, Value 2 = timestamp
	 * of the event as published to the Redis stream
	 */
	publish(streamName, event) {	
		logger.debug(`EventStoreClient.publish`);
		this._client.watch(event.id); //watch the id (account)
		return this._getAsync(event.id)  //fetch the current version from a Redis key with that ID 
		.then(result => {	
			if (!result || parseInt(result) === parseInt(event.version)) {  //key doesn't exist or versions match
				event.version += 1;  //increment version number prior to publishing the event
				logger.debug(`EventStoreClient.publish - streamName:${streamName}, event:${JSON.stringify(event)}\
				 - result:${result}`);
				return new Promise((resolve, reject) => {
					this._client.multi()  //atomic transaction that increments the version and adds event to stream
					.incr(event.id)
					.xadd(streamName, '*', 'event', JSON.stringify(event))
					.exec((err, replies) => {
						if (err) {
							reject(err);
						}
						else {
							resolve(replies);
						}
					});
				});
			}
			else {  //covers the scenario where a concurrent access causes a mismatch with event version numbers
					//return null and then it's up to the client to make another publish attempt
				return new Promise((resolve, reject) => {
					resolve(null);
				});
			}
		})
		.catch(err => {
			logger.error(`EventStoreClient.publish - streamName:${streamName}, event:${event} - ${err}`);
			throw err;
		});
	}
	
	/**
	 * Creates a subscription-type model for Redis streams.  Stream is read periodically via a redis readgroup and
	 * javascript interval.  Event emitter is then used to transmit the events to subscriber.
	 * @param {string} streamName - name of the Redis stream
	 * @param {string} consumerName - name of Redis stream consumer
	 * @return {array} - 2 values in an array, if successful. Null, if not.  Value 1 = new version number, Value 2 = timestamp
	 * of the event as published to the Redis stream
	 */
	subscribe(streamName, consumerName) {	
		let emitter;
		let groupName = streamName + 'Group';
		logger.debug(`EventStoreClient.subscribe - streamName:${streamName}, groupName:${groupName}, consumerName:${consumerName}`);
		
		if (this._emitters[streamName] && this._emitters[streamName][groupName]) {
			emitter = this._emitters[streamName][groupName];
		}
		else {
			this._client.xgroup('CREATE', streamName, groupName, '0', (err) => {});  //attempt to create Redis group
			emitter = new events.EventEmitter();
			let obj = setInterval(() => {
				this._readGroup(streamName, groupName, consumerName)
				.then(eventList => {
					if (eventList.length > 0) {
						emitter.emit('event', eventList);
					}
				})
				.catch(err => {
					logger.error(`EventStoreClient.subscribe - streamName:${streamName}, groupName:${groupName},\
					consumerName:${consumerName} - ${err}`);
					throw err;					
				});
			}, this._readInterval);
			if (!this._emitters[streamName]) {
				this._emitters[streamName] = {};
			}
			this._emitters[streamName][groupName] = emitter;
			this._intervals.push(obj);	
		}
		
		return emitter;
	}
	
	/**
	 * Private function to read new events from a Redis stream
	 * @private
	 * @param {string} streamName - name of redis stream
	 * @param {string} groupName - name of redis stream group
	 * @param {string} consumerName - name of redis stream consumer
	 * @return {array} array event objects
	 */
	_readGroup(streamName, groupName, consumerName) {
		logger.debug(`EventStoreClient._readGroup - streamName:${streamName}, groupName:${groupName}, consumerName:${consumerName}`);
		return this._xreadgroupAsync('GROUP', groupName, consumerName, 'STREAMS', streamName, '>')
		.then(results => {
			let eventList = [];
			if (results) {
				let events = results[0][1];
				for (let i=0; i<events.length; i++) {
					let obj = JSON.parse(events[i][1][1]);
					obj.timestamp = events[i][0];
					eventList.push(obj);
				}
			}
			logger.debug(`EventStoreClient._readGroup - eventList.length:${eventList.length}`);
			return eventList;
		})
		.catch(err => {
			logger.error(`EventStoreClient._readGroup - streamName:${streamName} - ${err}`);
			throw err;
		});
	}
};
