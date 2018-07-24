#!/usr/bin/env node
'use strict'

const {Publisher, Subscriber} = require('squeaky')
const {URL} = require('url')

const defaultOpts = {
  'h': {
    alias: 'host',
    type: 'string',
    default: `nsq://${process.env.NSQ_HOST || 'localhost'}:${Number(process.env.NSQ_PORT) || 4150}`,
    describe: 'the target nsq host and port'
  }
}

require('yargs')
  .usage('$0 <command> [args]')
  .command('publish [topic] [data]', 'publish an event', yargs => {
    yargs.positional('topic', {
      type: 'string',
      describe: 'the target nsq topic'
    })
    yargs.positional('data', {
      type: 'string',
      describe: 'the body of the event'
    })
    yargs.options({
      ...defaultOpts,
      'd': {
        alias: 'delay',
        type: 'number',
        default: 0,
        describe: 'an optional delay on publish'
      },
      't': {
        alias: 'timeout',
        type: 'number',
        default: 0,
        describe: 'an optional timeout for the event'
      }
    })

    if (process.stdin.isTTY) {
      yargs.demandOption(['topic', 'data'], 'please provide a topic and data (or pipe input data to the command.)')
    } else {
      yargs.demandOption(['topic'], 'please provide a topic.')
    }

    yargs.help()
  }, publish)
  .command('subscribe [topic] [channel]', 'subscribe to a channel', yargs => {
    yargs.positional('topic', {
      type: 'string',
      describe: 'the target nsq topic'
    })
    yargs.positional('channel', {
      type: 'string',
      describe: 'the event channel'
    })
    yargs.options({
      ...defaultOpts,
    })
  }, subscribe)
  .demandCommand()
  .help()
  .argv

async function publish ({topic, data, host, delay, timeout}) {
  const {hostname, port} = new URL(host)
  const publisher = new Publisher({
    host: hostname,
    port,
    timeout,
    maxConnectAttempts: 1,
    reconnectDelayFactor: 1,
    maxReconnectDelay: 0,
    autoConnect: false
  })

  try {
    await publisher.connect()
  } catch (err) {
    console.error('Could not connect to NSQ ("%s"), got error:', host)
    console.error(err.message)
  }

  if (!data) {
    if (process.stdin.isTTY) {
      throw new Error('an event body is required.')
    }

    const acc = []
    await {
      then (resolve, reject) {
        process.stdin.on('data', xs => acc.push(xs))
        process.stdin.once('end', resolve)
        process.stdin.once('error', reject)
      }
    }
    data = Buffer.concat(acc)
  }

  if (delay) {
    await publisher.publish(topic, data, delay)
  } else {
    await publisher.publish(topic, data)
  }

  await publisher.close()
}

async function subscribe ({topic, channel, host}) {
  const {hostname, port} = new URL(host)
  const subscriber = new Subscriber({
    host: hostname,
    port,
    topic,
    channel,
    maxConnectAttempts: 1,
    reconnectDelayFactor: 1,
    maxReconnectDelay: 0,
    autoConnect: false
  })

  try {
    await subscriber.connect()
  } catch (err) {
    console.error('Could not connect to NSQ ("%s"), got error:', host)
    console.error(err.message)
  }

  subscriber.on('message', msg => {
    if (Buffer.isBuffer(msg.body)) {
      process.stdout.write(msg.body)
    } else if (typeof msg === 'string') {
      console.log(msg.body)
    } else {
      console.log(JSON.stringify(msg.body))
    }
    msg.finish()
  })
}
