const utils = require('./helpers/utilHelper')
const ap = require('amqplib')
const Promise = require('bluebird')
const aliyunAmqpCli = require('aliyun-amqp-node-cli')
let aliConfig = null
const logger = require('./helpers/logHelper').getLogger('MQ')

// 阿里专业版每个connection可建64个channel（即可用于64个队列）
const mqConns = {}
exports.mqConns = mqConns

const to = async (promise) => {
  return promise.then(data => {
    return [null, data]
  })
    .catch(err => [err])
}

const getConn = async (vhostName, role, recoverFunc, args) => {
  utils.objArgsCheck({
    vhostName, role, recoverFunc, args
  }, ['vhostName', 'role', 'recoverFunc', 'args'])
  // 阿里建议consumer和producer用不同链接
  const connKey = vhostName + role
  const thisConn = mqConns[connKey]
  // 复用链接
  if (thisConn && thisConn.connection) {
    // 保存引用的方法，断连时刷新
    if (recoverFunc) {
      mqConns[connKey].recoverFuncList.push({
        recoverFunc,
        args
      })
    }
    return thisConn.connection
  }
  const amqplib = aliyunAmqpCli(aliConfig)(ap)
  const [err, connection] = await to(amqplib.connect(`amqp://${aliConfig.host}/${vhostName}?frameMax=0&heartbeat=10`))
  if (err) throw new Error(`MQ: ${connKey}, connection error`)
  // 链接成功后，重置链接对象
  mqConns[connKey] = {
    connection,
    channel: {},
    // 所有监听链接的方法（即channel）
    recoverFuncList: [{
      recoverFunc,
      args
    }]
  }
  logger.info('MQ connection success:', connKey)
  // 链接异常监听
  connection.on('error', async err => {
    logger.error(`MQ: ${connKey}, connection error:`, err.toString())
    // error后也会触发close事件，就不用init了
  })
  connection.on('close', async () => {
    logger.error(`MQ: ${connKey}, connection closed, reconnecting...`)
    delete mqConns[connKey].connection
    // 断连时, 接龙式刷新所有信道监听，避免并发创建多余链接
    await Promise.map(mqConns[connKey].recoverFuncList, async obj => obj.recoverFunc(...obj.args), {concurrency: 1})
  })
  connection.on('blocked', async () => {
    // 链接被阻塞，无法运转，可能是内存/CPU/磁盘出现了问题，这里可加监控
    logger.error(`MQ: ${connKey}, connection blocked...`)
  })
  return connection
}

const getLocalConn = async (connKey) => {
  // 断连后，重连需要时间缓冲，重试4次
  let retryNum = 0
  while (!mqConns[connKey] && retryNum <= 4) {
    await Promise.delay(1000)
    retryNum++
  }
  if (!mqConns[connKey]) throw new Error('connection获取失败')
  return mqConns[connKey]
}

const closeConn = async (vhostName, role) => {
  utils.objArgsCheck({vhostName, role}, ['vhostName', 'role'])
  const connKey = vhostName + role
  const thisConn = mqConns[connKey]
  if (!thisConn) return
  await thisConn.connection.close()
  delete mqConns[connKey]
}

const getConfirmChannel = async (connKey, queueName) => {
  utils.objArgsCheck({connKey, queueName}, ['connKey', 'queueName'])
  const localConn = await getLocalConn(connKey)
  const thisChannel = localConn.channel[queueName]
  if (thisChannel) return thisChannel
  const channel = await localConn.connection.createConfirmChannel()
  // 定义队列
  await channel.assertQueue(queueName, {durable: true})
  localConn.channel[queueName] = channel
  logger.info(`channel init success, ${connKey}>${queueName}`)
  channel.on('return', msg => {
    logger.error(`${connKey}>${queueName} channel msg returned: ${msg.content.toString()}`)
  })
  channel.on('error', err => {
    logger.error(`${connKey}>${queueName} channel err: ${err.toString()}`)
  })
  channel.on('close', async () => {
    logger.error(`${connKey}>${queueName} channel closed `)
  })
  return channel
}

const startProduce = async (queue) => {
  utils.objArgsCheck({queue}, ['queue'])
  await getConn(queue.vhostName, 'Producer', startProduce, [queue])
  await getConfirmChannel(queue.vhostName + 'Producer', queue.name)
}

const startConsume = async (queue, consumeFunc) => {
  utils.objArgsCheck({queue, consumeFunc}, ['queue', 'consumeFunc'])
  await getConn(queue.vhostName, 'Consumer', startConsume, [queue, consumeFunc])
  const channel = await getConfirmChannel(queue.vhostName + 'Consumer', queue.name)
  // 预加载1个消息
  await channel.prefetch(parseInt(queue.prefetch || 1))
  // 监听并消费通知队列
  await channel.consume(queue.name, async (msg) => {
    const contentStr = msg.content.toString()
    const message = JSON.parse(contentStr)
    try {
      await consumeFunc(message)
      channel.ack(msg)
      logger.info(`${queue.name} consume message success, `, message)
    } catch (e) {
      channel.nack(msg, false, true)
      logger.error(`${queue.name} consume message error, `, e)
    }
  }, {noAck: false})
}

const sendMsg = async (queue, msg) => {
  utils.objArgsCheck({queue, msg}, ['queue', 'msg'])
  // channel启动时 startProducer 已初始化，这里都会复用
  const channel = await getConfirmChannel(queue.vhostName + 'Producer', queue.name)
  return Promise.fromCallback((cb) => {
    channel.sendToQueue(queue.name, Buffer.from(JSON.stringify(msg)), {
      mandatory: true
    }, (err) => {
      if (err) {
        logger.error(`${queue.name} sendMsg error: ${err.toString()}`)
        return cb(err)
      }
      logger.debug(`${queue.name} sendMsg success: ${JSON.stringify(msg)}`)
      return cb()
    })
  })
}

const startFanoutProduce = async (queue) => {
  utils.objArgsCheck({queue}, ['queue'])
  await getConn(queue.vhostName, 'Producer', startFanoutProduce, [queue])
  await getFanoutChannel(queue.vhostName + 'Producer', queue)
}

const getFanoutChannel = async (connKey, quene) => {
  utils.objArgsCheck({connKey, queueName: quene.name, exchange: queue.exchange}, ['connKey', 'queueName', 'exchange'])
  const localConn = await getLocalConn(connKey)
  const thisChannel = localConn.channel[quene.name]
  if (thisChannel) return thisChannel
  const channel = await localConn.connection.createChannel()
  // 定义交换器
  await channel.assertExchange(quene.exchange, 'fanout')
  localConn.channel[quene.name] = channel
  logger.info(`fanoutChannel init success, ${connKey}>${quene.name}`)
  channel.on('return', msg => {
    logger.error(`${connKey}>${quene.name} channel msg returned: ${msg.content.toString()}`)
  })
  channel.on('error', err => {
    logger.error(`${connKey}>${quene.name} channel err: ${err.toString()}`)
  })
  channel.on('close', async () => {
    logger.error(`${connKey}>${quene.name} channel closed `)
  })
  return channel
}

const startFanoutConsume = async (queue, consumeFunc) => {
  // 注意：不同的项目，启动不同的队列来监听广播，即每个项目queue.name不能相同
  utils.objArgsCheck({queue, consumeFunc, exchange: queue.exchange}, ['queue', 'consumeFunc', 'exchange'])
  await getConn(queue.vhostName, 'Consumer', startConsume, [queue, consumeFunc])
  const channel = await getFanoutChannel(queue.vhostName + 'Consumer', queue)
  // 定义队列
  await channel.assertQueue(queue.name)
  await channel.bindQueue(queue.name, queue.exchange)
  // 预加载1个消息
  await channel.prefetch(parseInt(queue.prefetch || 1))
  // 监听并消费通知队列
  await channel.consume(queue.name, async (msg) => {
    const contentStr = msg.content.toString()
    const message = JSON.parse(contentStr)
    try {
      await consumeFunc(message)
      channel.ack(msg)
      logger.info(`${queue.name} consume message success, `, message)
    } catch (e) {
      channel.nack(msg, false, true)
      logger.error(`${queue.name} consume message error, `, e)
    }
  //  广播下 消息只负责发出去，收不到就没了
  }, {noAck: false})
}

const sendFanoutMsg = async (queue, msg) => {
  utils.objArgsCheck({queue, msg}, ['queue', 'msg'])
  const channel = await getFanoutChannel(queue.vhostName + 'Producer', queue)
  return Promise.fromCallback((cb) => {
    // args: 交互器、routingKey、消息体
    channel.publish(queue.exchange, queue.routingKey || '', Buffer.from(JSON.stringify(msg)), {
      mandatory: true
    }, (err) => {
      if (err) {
        logger.error(`${queue.name} sendMsg error: ${err.toString()}`)
        return cb(err)
      }
      logger.debug(`${queue.name} sendMsg success: ${JSON.stringify(msg)}`)
      return cb()
    })
  })
}


const funcs = {
  getConn,
  closeConn,
  startProduce,
  startConsume,
  getConfirmChannel,
  sendMsg,
  startFanoutProduce,
  startFanoutConsume,
  getFanoutChannel,
  sendFanoutMsg
}

const initFunc = (config) => {
  utils.objArgsCheck(config, ['accessKeyId', 'accessKeySecret', 'resourceOwnerId', 'host'])
  aliConfig = config
  return funcs
}

module.exports = initFunc
