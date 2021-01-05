'use strict'

const _ = require('lodash')
const async = require('async')
const fs = require('fs')
const WrkBase = require('bfx-wrk-base')

class WrkWrkIceMongo extends WrkBase {
  constructor (conf, ctx) {
    super(conf, ctx)

    this.action = ctx.action

    this.loadConf('mongo.ice', 'ice')

    this.init()
    this.start()
  }

  export () {
    if (this._exporting) {
      return
    }

    const op = this.conf.ice[this.action]

    if (!op) {
      console.error('OP: not found')
      return
    }

    if (!this.status.progress) {
      this.status.progress = op.start
    }

    if (this.status.progress > (Date.now() - op.end)) {
      return
    }

    this._exporting = true

    const query = {
      t: { '$gte': this.status.progress, '$lt': this.status.progress + op.step }
    }

    async.auto({
      data: next => {
        this.dbMongo_m0.db.collection(op.collection).find(query).sort({ t: 1 }).toArray(next)
      },
      save: ['data', (res, next) => {
        if (!res.data.length) {
          return next()
        }

        fs.writeFile(`${__dirname}/../store/${op.collection}_${this.status.progress}.log`, _.map(res.data, d => JSON.stringify(d)).join("\n"), next)
      }],
      del: ['save', (res, next) => {
        if (!res.data.length) {
          return next()
        }

        this.dbMongo_m0.db.collection(op.collection).deleteMany(query, next)
      }]
    }, (err, res) => {
      if (err) {
        console.error(err)
      } else {
        console.log(`EXPORTED(${this.action}): ${new Date(this.status.progress)} + ${op.step / 1000}secs, ${res.data.length} entries`)
        this.status.progress += op.step
        this.saveStatus()
      }

      this._exporting = false
    })
  } 

  init () {
    super.init()

    this.setInitFacs([
      ['fac', 'bfx-facs-db-mongo', 'm0', 'm0', {}]
    ])
  }

  _start (cb) {
    async.series([
      next => { super._start(next) },
      next => {
        this.interval_0.add('export', this.export.bind(this), 2500)
        next()
      }
    ], cb)
  }
}

module.exports = WrkWrkIceMongo
