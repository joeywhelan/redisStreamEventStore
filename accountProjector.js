/**
 * @fileoverview MongoDB projector for an account
 * @author Joey Whelan <joey.whelan@gmail.com>
 */

/*jshint esversion: 6 */

'use strict';
'use esversion 6';
const os = require('os');
const EventStoreClient = require('./eventStoreClient');
const logger = require('./accountProjectorLogger');
const MongoClient = require('mongodb').MongoClient;
const REDIS_PORT = 6379;
const REDIS_HOST = 'localhost';
const STREAM_NAME = 'accountStream';
const CONSUMER_NAME = 'accountProjector:' + os.hostname() + '_' + process.pid;
const CONNECTION_URL = 'mongodb://admin:mongo@localhost:27017';
const DB_NAME = 'accountDB';
const COLLECTION = 'accountCollection';
const READ_INTERVAL = 10000; //10 seconds, interval for checking subscription for new events
const PENDING_INTERVAL = 30000;  //30 seconds, interval on checking the pending queue of events

const esClient = new EventStoreClient(REDIS_PORT, REDIS_HOST, READ_INTERVAL);

/**
 * Helper function that chains promises to update a MongoDB and submit a Redis Ack for an event
 * @param {string} accountId - unique id of account
 * @param {string} timestamp - timestamp of the event
 * @param {object} db - MongoDB database connection
 * @param {object} query - query object for the db update
 * @param {object} value - value object for the db update
 * @return {promise}
 */
function update(accountId, timestamp, db, query, value, upsert) {
	logger.debug(`AccountProjector, update - accountId:${accountId}, timestamp:${timestamp}, query:${JSON.stringify(query)}, value:${JSON.stringify(value)}`);
	
	return db.collection(COLLECTION).updateOne(query, value, {'upsert' : upsert})
	.then((result) => {
		return esClient.ack(STREAM_NAME, timestamp);
	})
	.catch(err => {
		if (err.code === 11000) {  //covers a MongoDB bug.  scenario is two more simulatneous attempts at an update with
								   //upsert = true on the same unique id.
			console.log('11000 error');
			return update(accountId, timestamp, db, query, value, false);
		}
		else {
			throw err;
		}
	});	
}

/**
 * Function called from an event emitter that serves as subscriber to a Redis stream.  Each event represents
 * a message to a Redis stream for an account.  The function determines the event type, updates the fund value accordingly,
 * and submits the update to a MongoDB + Ack's the Redis stream for that message.
 * @param {array} eventList - array of event objects
 * @return none
 */
function eventHandler(eventList) {
	logger.debug(`AccountProjector, eventHandler - number of events received:${eventList.length}`);

	let connection;
	MongoClient.connect(CONNECTION_URL, {'useNewUrlParser' : true})
	.then((result) => {
		connection = result;
		const db = connection.db(DB_NAME);
		let promises = [];
		
		eventList.forEach((event) => {
			let amount;
			switch (event.type) {
				case 'create':
					amount = 0;
					break;
				case 'deposit':
					amount = event.amount;
					break;
				case 'withdraw':
					amount = -event.amount;
					break;
			}
			//this query filters to the particular account id AND ensures there's not been update made with this
			//timestamp twice
			let query = {$and: [{'_id':event.id}, {'timestamps':{$not: {$elemMatch:{$eq: event.timestamp}}}}]};
			//increment the funds and add the timestamp to the array of completed events
			let value = { $inc : {'funds' : amount}, $addToSet : {'timestamps' : event.timestamp}};
			promises.push(update(event.id, event.timestamp, db, query, value, true));
		});
		return Promise.all(promises);
	})
	.then((results) => {
		logger.debug(`AccountProjector, eventHandler - number of events handled:${results.length}`);
	})
	.catch((err) => {
		logger.error(`AccountProjector, eventHandler - ${err}`);
		throw err;
	})
	.finally(_ => {
		if (connection) {
			connection.close();
		}
	});
}

/**
 * Function for fetching all the events in the pending queue.  These would be events that are stuck
 * in processing due to some failure.  This is called out of a setInterval function with the interval being
 * the value of PENDING_INTERVAL
 * @return {array} - array of event objects
 */
function processPending() {
	//fetch those events that have aged = PENDING_INTERVAL in the pending queue
	logger.debug(`AccountProjector, processPending`);
	esClient.getPending(STREAM_NAME, CONSUMER_NAME, PENDING_INTERVAL) 
	.then((eventList) => {
		logger.debug(`AccountProjector, processPending - eventList.length:${eventList.length}`);
		eventHandler(eventList);
	})
	.catch((err) => {
		logger.error(`AccountProjector, processPending - ${err}`);
		throw err;
	});
}


/** @desc Account projector */
module.exports = class AccountProjector {
	
	constructor() {
		logger.debug(`AccountProjector constructor `);		
		//sets up a timer to check the pending queue
		this._interval = setInterval(processPending, PENDING_INTERVAL);  
	}
	
	/**
	 * Function for releasing resources (Redis connection and time interval object
	 */
	close() {
		logger.debug(`AccountProjector.close`);
		clearInterval(this._interval);
		esClient.close();
	}
	
	/**
	 * Function creates Redis connection through eventStoreClient and set ups an event listener
	 * for Redis stream events
	 */
	connect() {
		logger.debug(`AccountProjector.connect`);
		esClient.connect();
		this._emitter = esClient.subscribe(STREAM_NAME, CONSUMER_NAME);
		this._emitter.on('event', eventHandler);
	}
};	


const AccountProjector = require('./accountProjector');
const projector = new AccountProjector();
projector.connect();
