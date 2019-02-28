"use strict";

const Trailpack = require("trailpack");
const lib = require("./lib");
const _ = require("lodash");
const joi = require("joi");
const config = require("./lib/config");
const TaskerUtils = require("./lib/Util.js");
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
  }

  async prepareQueue() {
    let taskerConfig = this.app.config.tasker;
    const profile = getWorkerProfile(taskerConfig);
    const isMasterInstance = !process.env.WORKER;

    // create the queue
    this.app.tasker = kue.createQueue();
    if (isMasterInstance) {
      this.app.tasker.on("job enqueue", job => {
        this.app.log.info(`Job enqueued #${job}`);
      });

      this.app.tasker.on("job complete", jobId => {
        this.app.log.debug(`Job completed #${jobId}`);
      })

      this.app.tasker.on("job failed", job => {
        this.app.log.warn(`Job failed #${job}`);
      })


      // Temporary trigger jobs
      this.app.log.debug('Trigger job on master')
      const uuid = require('uuid')
      for (var i=0; i < 35; i++) {
        const job = this.app.tasker.create('UpdateStatisticsTask', { id: uuid.v4(), body: 'my payload'}).ttl(50).attempts(3).save()                
      }     
    } else {
      if (profile.tasks && profile.tasks.length === 0) {
          this.app.log.info(`No tasks defined for this worker ${process.env.WORKER}`)
          return
      }

      for (var i=0; i < profile.tasks.length; i++) {
        const taskName = profile.tasks[i]
        this.app.log.debug(`Preparing processing for ${taskName}`)

        this.app.tasker.process(taskName, 10, async (job, ctx, done) => {
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
            this.app.log.debug('Succesfully executed the task: ', JSON.stringify(result))
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
