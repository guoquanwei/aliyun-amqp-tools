const http = require('http')
const logger = require('../helpers/logHelper').getLogger('amqp-tools')
const server = http.createServer()

const amqpTools = require('../../index')({
    "accessKeyId": "xxx",
    "accessKeySecret": "xxx",
    "resourceOwnerId": "xxx",
    "host": "xxxxx"
})

const queue = {
    "name": "bronn-task",
    "vhostName": "yc-test",
    "prefetch": 1
}

amqpTools.startConsume(queue, () => {})
amqpTools.startProduce(queue)
