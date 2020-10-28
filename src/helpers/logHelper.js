const winston = require('winston')
const moment = require('moment')
const _ = require('lodash')
// const INSTANCE_ID = process.env.NODE_APP_INSTANCE || '0'

/*
 winston 默认日志级别
{
  error: 0,
  warn: 1,
  info: 2,
  verbose: 3,
  debug: 4,
  silly: 5
}
 */

const LOG_LEVEL = process.env.LOG_LEVEL || 'debug'

const instanceMap = {}

const formatSplatArr = splatArr => {
  return _.reduce(splatArr, (init, it) => {
    try {
      it = JSON.parse(it)
    } catch (e) {
      // do nothing
    }
    return init + JSON.stringify(it)
  }, '')
}
const myFormat = winston.format.printf(option => {
  const { level, message, label, timestamp } = option
  return `${level} [${moment(timestamp).format('YYYY-MM-DD HH:mm:ss.SSS')}] [${label}]: ${message}`
})
const errorStackFormat = winston.format(info => {
  if (info instanceof Error) {
    return Object.assign({}, info, {
      message: `Error: ${info.message}, Stack: ${info.stack}`
    })
  }
  return info
})

const metaFormat = winston.format(option => {
  const splat = option[Symbol.for('splat')]
  if (!_.isEmpty(splat)) {
    option.message = option.message + ` ${formatSplatArr(splat)}`
  }
  return option
})

const getLogger = (category) => {
  if (instanceMap[category]) {
    return instanceMap[category]
  }
  instanceMap[category] = winston.createLogger({
    level: LOG_LEVEL,
    format: winston.format.combine(
      errorStackFormat(),
      metaFormat(),
      winston.format.label({ label: category }),
      winston.format.timestamp(),
      winston.format.json()
    ),
    exitOnError: false,
    transports: [
      new winston.transports.Console({
        format:
          winston.format.combine(winston.format.colorize(), myFormat)
      })
    ]
  })
  return instanceMap[category]
}

module.exports = {
  getLogger
}
