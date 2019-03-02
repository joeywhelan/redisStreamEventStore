/**
 * @fileoverview Aggregate for an account
 * @author Joey Whelan <joey.whelan@gmail.com>
 */

/*jshint esversion: 6 */

'use strict';
'use esversion 6';
const logger = require('./accountLogger');

/** @desc Account aggregate */
module.exports = class Account {
	
	constructor(id, version=0, timestamp=0) {
		logger.debug(`Account constructor - id:${id}, version:${version}, timestamp:${timestamp}`);
		this.id = id;
		this.version = version;
		this.timestamp = timestamp;
		this.funds = 0;	
	}
	
	/**
	 * Performs a funds deposit to a player
	 *  Will generate an exception for a non-positive deposit amount.
	 * @param {int} amount - amount to be deposited
	 * @return void
	 */
	deposit(amount) {
		logger.debug(`Account.deposit - amount:${amount}`);
		if (amount <= 0) {
			throw new Error('Attempting a transaction with a 0 or negative value');
		}
		
		this.funds += amount;
		return;
	}
	
	/**
	 * 	Updates the aggregate from a list of events.  
	 * @param {array} events - list of event objects
	 * @return none
	 */
	rehydrate(events) {
		logger.debug(`Account.rehydrate - events.length:${events.length}`);
		for (let event of events) {
			if (event.id === this.id && event.timestamp !== this.timestamp) { //only apply events that have happened since last timestamp on agg
				this.version = event.version;
				this.timestamp = event.timestamp;
				if (event.hasOwnProperty('amount') && event.hasOwnProperty('type')) {
					switch (event.type) {
						case 'deposit':
							this.funds += event.amount;
							break;
						case 'withdraw':
							this.funds -= event.amount;
							break;
						default:
							break;
					}
				}
			}
		}
	}
	
	/**
	 * Helper function to generate an object from the account class
	 * @return {Object} object representing the current state of aggregate.
	 */
	toObject() {
		return {'id':this.id, 'version':this.version, 'timestamp':this.timestamp, 'funds':this.funds};
	}
	
	/**
	 * Performs a funds withdrawal from an account
	 *  Will generate an exception for a non-positive withdrawal amount or an attempt to overdraft
	 * @param {int} amount - amount to be withdrawn
	 * @return void
	 */
	withdraw(amount) {
		logger.debug(`Account.withdraw - amount:${amount}`);
		if (amount <= 0) {
			throw new Error('Attempting a transaction with a 0 or negative value');
		}
		
		if (this.funds - amount < 0) {
			throw new Error('Attempting to deduct more funds than available');
		}		
		
		this.funds -= amount;
		return;
	}
};
