/**
 * @fileoverview Test client for an account
 * @author Joey Whelan <joey.whelan@gmail.com>
 */

/*jshint esversion: 6 */

'use strict';
'use esversion 6';
const fetch = require('node-fetch');
const id = 'JohnDoe';

//Account Rest Service.  Create an account.
function test1() {
	const url = 'http://127.0.0.1:8444/accounts';
	const body = {'id' : id};
	
	return fetch(url, {
		method: 'POST',
		body: JSON.stringify(body),
		headers: {'Content-Type' : 'application/json; charset=utf-8'},
		cache: 'no-store',
		mode: 'cors'
	})
	.then(response => {
		if (response.ok) {
			return response.json();
		}
		else {
			const msg = 'Response status: ' + response.status;
			throw new Error(msg);
		}	
	})
	.then(json => {
		console.log('test1: ' + JSON.stringify(json));
	})
	.catch(err => { 
		console.log(err);
	});		
}

//Account Rest Service.  Fetch an account.
function test2() {
	const url = 'http://127.0.0.1:8444/accounts/' + id;
	
	return fetch(url, {
		method: 'GET',
		headers: {'Content-Type' : 'application/json; charset=utf-8'},
		cache: 'no-store',
		mode: 'cors'
	})
	.then(response => {
		if (response.ok) {
			return response.json();
		}
		else {
			const msg = 'Response status: ' + response.status;
			throw new Error(msg);
		}	
	})
	.then(json => {
		console.log('test2: ' + JSON.stringify(json));
	})
	.catch(err => { 
		console.log(err);
	});		
}

//Account Rest Service.  Deposit $100.
function test3() {
	const url = 'http://127.0.0.1:8444/accounts/' + id + '/deposits';
	const body = {'amount' : 100};
	
	return fetch(url, {
		method: 'POST',
		body: JSON.stringify(body),
		headers: {'Content-Type' : 'application/json; charset=utf-8'},
		cache: 'no-store',
		mode: 'cors'
	})
	.then(response => {
		if (response.ok) {
			return response.json();
		}
		else {
			const msg = 'Response status: ' + response.status;
			throw new Error(msg);
		}	
	})
	.then(json => {
		console.log('test3: ' + JSON.stringify(json));
	})
	.catch(err => { 
		console.log(err);
	});		
}

//Account Rest Service.  Withdraw $100.
function test4() {
	const url = 'http://127.0.0.1:8444/accounts/' + id + '/withdrawals';
	const body = {'amount' : 100};
	
	return fetch(url, {
		method: 'POST',
		body: JSON.stringify(body),
		headers: {'Content-Type' : 'application/json; charset=utf-8'},
		cache: 'no-store',
		mode: 'cors'
	})
	.then(response => {
		if (response.ok) {
			return response.json();
		}
		else {
			const msg = 'Response status: ' + response.status;
			throw new Error(msg);
		}	
	})
	.then(json => {
		console.log('test4: ' + JSON.stringify(json));
	})
	.catch(err => { 
		console.log(err);
	});		
}

test4();
