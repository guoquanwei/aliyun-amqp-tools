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
    if (!mqConns[connKey]) throw new Error('先定义connection')
    const thisChannel = mqConns[connKey].channel[queueName]
    if (thisChannel) return thisChannel
    const channel = await mqConns[connKey].connection.createConfirmChannel()
    // 定义队列
    await channel.assertQueue(queueName, {durable: true})
    mqConns[connKey].channel[queueName] = channel
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
    await getConn(queue.vhostName, 'Producer', startProducer, [queue])
    await getConfirmChannel(queue.vhostName + 'Producer', queue.name)
}

const startConsume = async (queue, consumeFunc) => {
    utils.objArgsCheck({queue, consumeFunc}, ['queue', 'consumeFunc'])
    await getConn(queue.vhostName, 'Consumer', startConsumer, [queue, consumeFunc])
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

const funcs = {
    getConn,
    closeConn,
    getConfirmChannel,
    startProduce,
    startConsume,
    sendMsg
}

const initFunc = (config) => {
    utils.objArgsCheck(config, ['accessKeyId', 'accessKeySecret', 'resourceOwnerId', 'host'])
    aliConfig = config
    return funcs
}

module.exports = initFunc
