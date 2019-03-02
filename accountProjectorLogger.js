/**
 * @fileoverview Winston logger
 * @author Joey Whelan <joey.whelan@gmail.com>
 */
/*jshint esversion: 6 */

'use strict';
'use esversion 6';

const winston = require('winston');

const logger = winston.createLogger({
	  transports: [
		  new winston.transports.Console(),
		  new winston.transports.File({filename: './accountProjector.log'})
	  ],
	  format: winston.format.combine(
		winston.format.timestamp(),
	    winston.format.align(),
	    winston.format.printf(info => `${info.timestamp} ${info.level}: ${info.message}`)
	  ),
	  level: 'debug'
	});

module.exports = logger;
