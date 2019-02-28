const Config = module.exports
const joi = require('joi')

Config.schema = joi.object().keys({
  profiles: joi.object(),
  connection: joi.object().keys({
    exchange: joi.string(),
    retryExchange: joi.string(),
    workQueueName: joi.string(),
    interruptQueueName: joi.string(),
    retryQueueName: joi.string(),
    user: joi.string(),
    pass: joi.string(),
    server: joi.array(),
    port: joi.number(),
    vhost: joi.string(),
    uri: joi.string(),
    timeout: joi.number(),
    heartbeat: joi.number(),
    failAfter: joi.number(),
    retryLimit: joi.number(),
    maxTaskRetries: joi.number(),
    taskRetryDelay: joi.number(),
  })

}).unknown()

Config.defaults = {
  profiles: {},
  connection: {
    server: ['localhost'],
    user: 'guest',
    pass: 'guest',
    port: 5672,
    vhost: '%2f'
  },
  maxTaskRetries: 5,
  taskRetryDelay: 1000,
  concurrentTasks: 5,
  worker: false
}
