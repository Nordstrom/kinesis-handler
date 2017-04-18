'use strict'

/* eslint-env node, mocha */

// const expect = require('chai').expect;

// const catalog = require('./kinesisHandler.js');

describe('Kinesis Handler Unit Tests', () => {
  let consoleLog
  before(() => {
    consoleLog = console.log
    console.log = () => {}
  })
  after(() => {
    console.log = consoleLog
  })
  it('should have some tests', () => {
  })
})
