'use strict'
const _ = require('lodash');

module.exports = class Task {
  constructor (app, message, context, job) {
    this.app = app
    this.message = message
    this.context = context
    this.job = job
    this.id = message.id
    this.isAcknowledged = false
    this.app.log.debug('Task() context: ', context)
  }
  
  run () {
    throw new Error('Subclasses must override Task.run')
  }

  run (msg) {
    console.log('Task Retry:', msg)
  }


  interrupt (msg) {
    console.log('Task Interrupt:', msg)
  }

  retry (msg) {
    console.log('Task Retry:', msg)
  }

  finalize (results) {
    console.log('Task Finalize:', results)
  }
}
