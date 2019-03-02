/**
 * @fileoverview Web server providing a REST interface to the Account microservice.
 * @author Joey Whelan <joey.whelan@gmail.com>
 */
/*jshint esversion: 6 */
'use strict';
'use esversion 6';


const express = require('express');
const jsonParser = express.json();
const app = express();
const AccountService = require('./accountService');
const logger = require('./accountLogger');
const REDIS_PORT = 6379;
const REDIS_HOST = 'localhost';
const LISTEN_PORT = 8444;

const service = new AccountService(REDIS_PORT, REDIS_HOST);

/**
 * Provides the 'retrieve' function for an account aggregate
 * @param {string} id - ID of account to be retrieved
 * @return {Object} if found - 200 status w/account JSON object.  otherwise, 404 with empty object
 */
app.get('/accounts/:id', (request, response) => {
	service.fetch(request.params.id)
	.then(result => {
		if (result) {
			response.status(200).json(result);
		}
		else {
			response.status(404).json({'errorMessage' : 'not found'});
		}
	})
	.catch(err => {
		response.status(400).json({'errorMessage' : err.message});
	});
});

/**
 * Provides the 'create' function for an account aggregate
 * @param {string} id - ID of account to be created
 * @return {Object} - JSON object of the newly created account
 */
app.post('/accounts', jsonParser, (request, response) => {
	service.create(request.body.id)
	.then(result => {
		if (result) {
			response.status(201).json(result);
		}
	})
	.catch(err => {
		response.status(400).json({'errorMessage' : err.message});
	});
});

/**
 * Function for placing a deposit transaction against an account (ID)
 * @param {string} id - ID of account for the deposit.  Query parameter
 * @param {Object} amount - JSON object containing the deposit amount.  Body parameter
 * @return {Object} - successful - 200 status w/JSON object containing the ID and amount,
 * 					if there's a concurrency (version) conflict during the deposit attempt, 409 returned
 * 					erroneous deposit attempt - 400 returned
 */
app.post('/accounts/:id/deposits', jsonParser, (request, response) => {
	service.deposit(request.params.id, request.body.amount)
	.then(result => {
		if (result && result.hasOwnProperty('amount')) {
			response.status(200).json(result);
		}
		else {
			response.status(409).json({'errorMessage' : 'Conflict while attempting deposit'});
		}
	})
	.catch(err => {
		response.status(400).json({'errorMessage' : err.message});
	});	
});

/**
 * Function for placing a withdrawal transaction against an account (ID)
 * @param {string} id - ID of account for the withdrawal.  Query parameter
 * @param {Object} amount - JSON object containing the withdrawal amount.  Body parameter
 * @return {Object} - successful - 200 status w/JSON object containing the ID and amount,
 * 					if there's a concurrency (version) conflict during the withdrawal attempt, 409 returned
 * 					erroneous withdrawal attempt - 400 returned
 */
app.post('/accounts/:id/withdrawals', jsonParser, (request, response) => {
	service.withdraw(request.params.id, request.body.amount)
	.then(result => {
		if (result && result.hasOwnProperty('amount')) {
			response.status(200).json(result);
		}
		else {
			response.status(409).json({'errorMessage' : 'Conflict while attempting withdrawal'});
		}
	})
	.catch(err => {
		response.status(400).json({'errorMessage' : err.message});
	});	
});


app.listen(LISTEN_PORT);
logger.info(`Account microservice - started on port ${LISTEN_PORT}`);
