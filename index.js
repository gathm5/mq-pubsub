const { Message, Producer, Consumer } = require('redis-smq');

let mqConfig = {
  namespace: 'mq',
  redis: {
    host: '127.0.0.1',
    port: 6379,
    connect_timeout: 3600000,
  }
}

let events = {};
let producerMap = {};
let consumerMap = {};
let forceFromEventList = false;

const createConsumer = (name, callback) => {
  class QueueConsumer extends Consumer {
    consume(message, cb) {
      cb();
      callback(message);
    }
  }
  QueueConsumer.queueName = name;
  return QueueConsumer;
}

module.exports = {
  EVENTS: () => {
    return Object.keys(events);
  },
  configure(NS, options = { supportedEvents: [], db: {}, monitor: undefined }) {
    mqConfig = {
      namespace: NS || mqConfig.namespace,
      redis: {
        ...mqConfig.redis,
        ... (options.db || {})
      },
      log: {
        enabled: 0,
        options: {
          level: 'trace',
        },
      },
      monitor: options.monitor,
    };
    if (Array.isArray(options.supportedEvents) && options.supportedEvents.length > 0) {
      forceFromEventList = true;
      options.supportedEvents.forEach((v) => {
        if (v) {
          events[v] = v;
        }
      })
    }
    return true;
  },
  publish(event, payload) {
    return new Promise((resolve, reject) => {
      if (!event || (forceFromEventList && !events[event])) {
        return reject(Error('Provide an event name'));
      }

      const producer = producerMap[event] || new Producer(event, mqConfig);
      producerMap[event] = producer;

      const message = new Message();
      message.setBody(payload);
      producer.produceMessage(message, (err) => {
        if (err) return reject(err);
        resolve(message);
      });

    });
  },
  async subscribe(event, callback) {
    try {
      if ((!event && typeof callback !== 'function') || (forceFromEventList && !events[event])) {
        throw Error('Provide valid event name and callback function');
      }
      if (consumerMap[event]) {
        return {
          status: 204,
          eventName: event,
          subscribed: true,
          message: `previously subscribed to ${event}`,
        };
      }
      const consumer = consumerMap[event] || new (createConsumer(event, callback))(mqConfig, { messageConsumeTimeout: 2000 });
      consumerMap[event] = consumer;
      consumer.run();
      return {
        status: 200,
        eventName: event,
        subscribed: true,
        message: `subscribed to ${event}`,
      };
    } catch (e) {
      return {
        status: 500,
        reason: e.message,
        subscribed: false,
      }
    }
  },
  unpublish(event) {
    if (event && producerMap[event]) {
      producerMap[event].shutdown();
      delete producerMap[event];
    }
  },
  unsubscribe(event) {
    if (event && consumerMap[event]) {
      consumerMap[event].stop();
      delete consumerMap[event];
    }
  },
  destroy() {
    let response = 'All producers & consumers are removed';
    try {
      Object.keys(producerMap).forEach(v => producerMap[v].shutdown());
      Object.keys(consumerMap).forEach(v => consumerMap[v].stop());
    } catch (e) {
      response = 'Partially cleared. But something did go wrong';
    }
    producerMap = {};
    consumerMap = {};
    return response;
  }
};
