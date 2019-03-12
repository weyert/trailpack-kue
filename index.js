"use strict";

const Trailpack = require("trailpack");
const lib = require("./lib");
const _ = require("lodash");
const joi = require("joi");
const config = require("./lib/config");
const kue = require("kue");

module.exports = class KueTrailpack extends Trailpack {
  /**
   * TODO document method
   */
  validate() {
    this.app.config.tasker = _.defaultsDeep(
      this.app.config.tasker,
      config.defaults
    );
    return new Promise((resolve, reject) => {
      joi.validate(this.app.config.tasker, config.schema, (err, value) => {
        if (err) {
          return reject(new Error("Tasker Configuration: " + err));
        }

        return resolve(value);
      });
    });
  }

  /**
   * configure rabbitmq exchanges, queues, bindings and handlers
   */
  configure() {
    // what to do?
    this.app.tasker = {
      publish: (taskName, payload, options = {}) => {
        this.app.log.debug(`Publishing task ${taskName} queued for execution`)
        return new Promise((resolve, reject) => {
          const uuid = require('uuid')
          const jobId = uuid.v4()
          const job = this.app.kue.create(taskName, { id: uuid.v4(), body: payload})
          if (options && options.ttl) {
              job.ttl(options.ttl)
          }
          if (options && options.attempts) {
              job.attempts(options.attempts)
          }
          if (options.priority) {
              job.priority(options.priority)
          }
  
          if (options.backoff) {
              job.backoff(options.backoff)        
          }
          
          this.app.log.debug(`Enabling automatic cleanup for ${taskName} with job id: ${jobId}`)
          job.removeOnComplete(true)
          job.save(err => {
              if (err) {
                return reject(err)
              }

              resolve(jobId)
          })
        })
      }
    }
  }

  async prepareQueue() {
    let taskerConfig = this.app.config.tasker;
    const profile = getWorkerProfile(taskerConfig);
    const isMasterInstance = !process.env.WORKER;

    // create the queue
    this.app.kue = kue.createQueue({
      jobEvents: false,
      redis: taskerConfig.connection.uri,
    });

    if (isMasterInstance) {
      this.app.kue.on("job enqueue", job => {
        this.app.log.info(`Job enqueued #${job}`);
      });

      this.app.kue.on("job complete", jobId => {
        this.app.log.debug(`Job completed #${jobId}`);
      })

      this.app.kue.on("job failed", job => {
        this.app.log.warn(`Job failed #${job}`);
      })

      this.app.on( 'error', function( err ) {
        console.log( 'Oops... ', err );
      })

      this.app.kue.watchStuckJobs(1000)
    } else {
      if (profile.tasks && profile.tasks.length === 0) {
          this.app.log.info(`No tasks defined for this worker ${process.env.WORKER}`)
          return
      }

      for (var i=0; i < profile.tasks.length; i++) {
        const taskName = profile.tasks[i]
        const taskOptions = profile.taskOptions[taskName]
        const { maxProcessors = 10 } =  taskOptions
        this.app.log.debug(`Preparing processing for ${taskName}`)

        this.app.on( 'error',  err => {
          this.app.log.debug(`Something went wrong while processing job: %s`, err)
        })

        // this.app.kue.watchStuckJobs(1000)

        this.app.kue.process(taskName, maxProcessors, async (job, ctx, done) => {
          const { id, type: jobTaskName, data } = job.toJSON()
          this.app.log.debug(`Incoming job for task ${jobTaskName}: `, JSON.stringify(job))
          const TaskClass = this.app.api.tasks[jobTaskName]
          if (!TaskClass) {
            this.app.log.debug('Failed to lookup the task to be processed')
            done(null, new Error('Failed to lookup the task to be processed'))
          }

          const currentTask = new TaskClass(this.app, data, ctx, job)
          try {
            const result = await currentTask.run()
            this.app.log.debug(`Succesfully executed the task ${taskName} with id: ${id} result: %s`, JSON.stringify(result))
            const finished = await currentTask.finalize()
            done(null, true)
          } catch(err) {
            this.app.log.debug('Failed to succesfully run the task: ', err.message)
            done(null, err)
          }
        })
      }
    }
  }

  /**
   * Establish connection to the RabbitMQ exchange, listen for tasks.
   */
  initialize() {
    this.app.on("trails:ready", async () => {
      try {
        const result = await this.prepareQueue();
        this.app.log.debug("Result: ", result);
      } catch (err) {
        this.app.log.debug("Error: ", err.message);
      }
    });
  }

  unload () {
    this.app.log.info('Trailpack.unload(): Unloading the queue')
    return new Promise((resolve, reject) => {
      if (!this.app.kue) {
        return Promise.resolve()
      }

      this.app.kue.shutdown(5000, (err) => {
        if (err) {
          return reject(err)
        }

        console.log('Successfully terminated the kue')
        return resolve(true)
      })  
    })
  }

  constructor(app) {
    super(app, {
      config: require("./config"),
      api: require("./api"),
      pkg: require("./package")
    });
  }
};

/**
 * Get the profile for the current process
 * The profile contains a list of tasks that this process can work on
 * If there is no profile (ie the current process is not a worker process), this returns undefined
 */
function getWorkerProfile(taskerConfig) {
  const profileName = taskerConfig.worker;

  if (!profileName || !taskerConfig.profiles[profileName]) {
    return { tasks: [] };
  }

  return taskerConfig.profiles[profileName];
}

module.exports.Task = lib.Task;
