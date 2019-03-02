/**
 * @fileoverview Service for an account
 * @author Joey Whelan <joey.whelan@gmail.com>
 */

/*jshint esversion: 6 */

'use strict';
'use esversion 6';
const EventStoreClient = require('./eventStoreClient');
const Account = require('./account');
const logger = require('./accountLogger');


/** @desc Account service */
module.exports = class AccountService {
	
	constructor(redisPort, redisHost) {
		this._client = new EventStoreClient(redisPort, redisHost);
		this._client.connect();
		this._accounts = {}; //cache for account.  map object containing account objects
	}
	
	/**
	 * Provides the 'create' function for account aggregate.  Attempts to 'add' a new ID to the system.  If the ID is unique,
	 * an event is published.  If it is not, an error is returned
	 * @param {string} id - ID of account to be created
	 * @return {Object} - object containing the newly created account id
	 */
	create(id) {
		return this._client.addId(id, 'accountId')
		.then(isUnique => {
			logger.debug(`AccountService.create - id:${id} - isUnique:${isUnique}`);
			if (isUnique) {
				const newEvent = {'id' : id, 'version': 0, 'type': 'create'};
				return this._client.publish('accountStream', newEvent);
			}
			else {
				return new Promise((resolve, reject) => {
					resolve(null);
				});
			}
		})
		.then (results => {
			if (results && results.length === 2) {  //results is an array.  first item is the new version number of the aggregate, 
													//second is the timestamp of the create event that was published
				logger.debug(`AccountService.create - id:${id} - results:${results}`);
				const version = results[0];
				const timestamp = results[1];
				const account = new Account(id, version, timestamp);
				this._accounts[id] = account;  //add the new account to the cache
				return {'id' : id};
			}
			else {
				throw new Error('Attempting to create an account id that already exists');
			}
		})
		.catch(err => {
			logger.error(`AccountService.create - id:${id} - ${err}`);
			throw err;
		});
	}
	
	/**
	 * Function for placing a deposit transaction against an account (ID)
	 * @param {string} id - ID of account for the deposit.
	 * @param {int} amount - deposit amount.
	 * @return {Object} - successful - object containing the ID and amount,
	 * 						if there's a concurrency conflict or other error, a null object will be returned.
	 */
	deposit(id, amount) {
		let account;
		
		return this._loadAccount(id) //attempt to load the account from cache and/or rehydrate from events
		.then(result => {
			account = result;
			account.deposit(amount);
			const newEvent = {'id' : id, 'version' : account.version, 'type': 'deposit', 'amount': amount};
			return this._client.publish('accountStream', newEvent);
		})
		.then(results => {
			logger.debug(`AccountService.deposit - id:${id}, amount:${amount} - results:${results}`);
			if (results) {
				account.version = results[0];
				account.timestamp = results[1];
				this._accounts[id] = account; //update the account cache
				return {'id': id, 'amount': amount};
			}
			else {
				account.withdraw(amount); //rolling back aggregate due to unsuccessful publishing of deposit event
				return null;
			}
		})
		.catch(err => {
			logger.error(`PlayerService.deposit - id:${id}, amount:${amount} - ${err}`);
			throw err;
		});
	}
	
	/**
	 * Function for fetching an account object
	 * @param {string} id - ID of account.
	 * @return {Object} - successful -  account object
	 */
	fetch(id) {
		return this._loadAccount(id)
		.then(account => {
			return account.toObject();
		})
		.catch(err => {
			logger.error(`AccountService.get - id:${id} - ${err}`);
			throw err;
		});
	}
	
	/**
	 * Function for clean up.  Shuts down redis client in the event store.
	 * @return void
	 */
	close() {
		logger.debug(`AccountService.close`);
		this._client.close();
	}
	
	/**
	 * Function for placing a withdrawal transaction against an account (ID)
	 * @param {string} id - ID of account for the deposit.
	 * @param {int} amount - withdrawal amount.
	 * @return {Object} - successful - object containing the ID and amount,
	 * 						if there's a concurrency conflict or other error, a null object will be returned.
	 */
	withdraw(id, amount) {
		let account;
		
		return this._loadAccount(id)
		.then(result => {
			account = result;
			account.withdraw(amount);
			const newEvent = {'id' : id, 'version' : account.version, 'type': 'withdraw', 'amount': amount};
			return this._client.publish('accountStream', newEvent);
		})
		.then(results => {
			logger.debug(`AccountService.withdraw - id:${id}, amount:${amount} - results:${results}`);
			if (results) {
				account.version = results[0];
				account.timestamp = results[1];
				this._accounts[id] = account;
				return {'id': id, 'amount': amount};
			}
			else {
				account.deposit(amount); //rolling back aggregate due to unsuccessful publishing of withdraw event
				return null;
			}
		})
		.catch(err => {
			logger.error(`AccountService.withdraw - id:${id}, amount:${amount} - ${err}`);
			throw err;
		});
	}
	
	/**
	 * Private function for fetching an account object.  Attempts to load the account from cache.  Account object
	 * rehydrated from events from eventstore.  If no events are found for ID given, error is thrown.
	 * @private
	 * @param {string} id - ID of account.
	 * @return {Object} - successful - object containing the account
	 */
	_loadAccount(id) {
		let account;
		let inCache = false;
	
		if (this._accounts.hasOwnProperty(id)) {
			account = this._accounts[id];
			inCache = true;
		}
		else {
			account = new Account(id);
		}
		
		return this._client.get('accountStream', id, account.timestamp)
		.then(events => {
			logger.debug(`AccountService._loadAccount - id:${id} - events.length:${events.length}`);
			if (!inCache && events.length === 0) {
				throw new Error('Non-existent account id');
			}
			else {			
				account.rehydrate(events);	
				return account;
			}
		})
		.catch(err => {
			logger.error(`AccountService._accountPlayer - id:${id} - ${err}`);
			throw err;
		});
	}
};
