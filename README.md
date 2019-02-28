# trailpack-kue

[![NPM version][npm-image]][npm-url]
[![Build status][ci-image]][ci-url]
[![Dependency Status][daviddm-image]][daviddm-url]
[![Code Climate][codeclimate-image]][codeclimate-url]

Easily set up background workers with [Kue](https://www.rabbitmq.com/) and [Trails](http://trailsjs.io).

## Install

```sh
$ npm install --save trailpack-kue
```

## Configure

### Add Trailpack

```js
// config/main.js
module.exports = {
  packs: [
    // ... other trailpacks
    require('trailpack-kue')
  ]
}
```

### Configure Tasker Settings

```js
// config/tasker.js
module.exports = {

  /**
   * Define worker profiles. Each worker of a given type listens for the
   * "tasks" defined in its profile below. The task names represent a Task
   * defined in api.services.tasks. Note that 'memoryBound' and 'cpuBound' are
   * arbitrary names.
   */
  profiles: {
    memoryBound: {
      tasks: [ 'hiMemoryTask1' ]
    },
    cpuBound: {
      tasks: [ 'VideoEncoder', 'hiCpuTask2' ]
    }
  },

  /**
   * Limit the amount of concurrent tasks, depending on the amount of workers increase this value.
   */
  concurrentTasks: 5,

  /**
   * Set worker to subscribe to tasks in the matching profile (tasker.profiles).
   * If process.env.WORKER does not match a profile, the application will not subscribe to any tasks
   */
  worker: process.env.WORKER
}
```

### Configure `worker` Environment

```js
// config/env/worker.js
module.exports = {
  main: {

    /**
     * Only load the packs needed by the workers
     */
    packs: [
      require('trailpack-tasker2')
    ]
  }
}
```

If the worker profiles each require more granular environment configurations,
create `worker-cpuBound`, `worker-memoryBound`, etc. environments.

### Include tasks in the app object
Create a directory `api/tasks`.  Any task definitions will be created as classes in this directory.
Create  `api/tasks/index.js` to export all of the tasks.
Include this directory in `api/index.js`.  Here is an example:
```js
// api/index.js

exports.controllers = require('./controllers')
exports.models = require('./models')
exports.policies = require('./policies')
exports.services = require('./services')
exports.tasks = require('./tasks')
```

## Usage

Define tasks in `api.tasks`.  Tasks are run by a worker processes.

```js
// api/tasks/VideoEncoder.js

const Task = require('trailpack-kue').Task
module.exports = class VideoEncoder extends Task {

  /**
   * "message" is the message from RabbitMQ, and contains all the information
   * the worker needs to do its job. By default, sets this.message and this.app.
   *
   * @param message.body.videoFormat
   * @param message.body.videoBuffer
   */
  constructor (app, message) {
    super(app, message)
  }

  /**
   * Do work here. When the work is finished (the Promise is resolved), send
   * "ack" to the worker queue. You must override this method.
   *
   * @return Promise
   */
  run () {
    return doWork(this.message)
  }

  /**
   * This is a listener which is invoked when the worker is interrupted (specifically,
   * an interrupt is a particular type of message that instructs this worker to
   * stop).
   */
  interrupt () {
    this.log.warn('only encoded', this.currentIndex, 'out of', this.totalItems, 'frames')
  }

  /**
   * Perform any necessary cleanup, close connections, etc. This method will be
   * invoked regardless of whether the worker completed successfully or not.
   * @return Promise
   */
  finalize () {
    return doCleanup(this.message)
  }
}
```


To start a task, publish a message via the `app.tasker` interface: 
```
const taskId = app.tasker.publish('VideoEncoder', { vidoeUrl: 'http://...' }
```

To interrupt a task in progress, use the `taskId` that is returned from app.tasker.publish(..):
```
app.tasker.cancel('VideoEncoder', taskId)
```



## Deployment

An example [Procfile](https://devcenter.heroku.com/articles/procfile) may look like:

```
web: npm start
memoryBound: NODE_ENV=worker WORKER=memoryBound npm start
cpuBound: NODE_ENV=worker WORKER=cpuBound npm start
```


## License
MIT

[npm-image]: https://img.shields.io/npm/v/trailpack-tasker2.svg?style=flat-square
[npm-url]: https://npmjs.org/package/trailpack-tasker2
[ci-image]: https://img.shields.io/travis/JAertgeerts/trailpack-tasker/master.svg?style=flat-square
[ci-url]: https://travis-ci.org/JAertgeerts/trailpack-tasker
[daviddm-image]: http://img.shields.io/david/jaertgeerts/trailpack-tasker.svg?style=flat-square
[daviddm-url]: https://david-dm.org/jaertgeerts/trailpack-tasker
[codeclimate-image]: https://img.shields.io/codeclimate/github/JAertgeerts/trailpack-tasker.svg?style=flat-square
[codeclimate-url]: https://codeclimate.com/github/JAertgeerts/trailpack-tasker

