'use strict'

const AJV = require('ajv')

const constants = {
  // generic reference to self in error messages
  MODULE: 'lib/kinesisHandler.js',
  // methods
  METHOD_PROCESS_EVENT: 'processEvent',
  METHOD_PROCESS_KINESIS_EVENT: 'processKinesisEvent',
  // errors
  BAD_MSG: 'bad msg:', // These messages aren't bad, so much as misunderstood (not validating with a known schema).  TODO make class of Errors.
}

function makeSchemaId(schema) {
  return `${schema.self.vendor}/${schema.self.name}/${schema.self.version}`
}

/**
 * Example Kinesis Event Batch:
 * {
 *   "Records": [
 *     {
 *       "kinesis": {
 *         "kinesisSchemaVersion": "1.0",
 *         "partitionKey": "undefined",
 *         "sequenceNumber": "49568749374218235080373793662003016116473266703358230578",
 *         "data": "eyJzY2hlbWEiOiJjb20ubm9yZHN0cm9tL3JldGFpb[...]Y3NDQiLCJjYXRlZ29yeSI6IlN3ZWF0ZXJzIGZvciBNZW4ifX0=",
 *         "approximateArrivalTimestamp": 1484245766.362
 *       },
 *       "eventSource": "aws:kinesis",
 *       "eventVersion": "1.0",
 *       "eventID": "shardId-000000000003:49568749374218235080373793662003016116473266703358230578",
 *       "eventName": "aws:kinesis:record",
 *       "invokeIdentityArn": "arn:aws:iam::515126931066:role/devProductCatalogReaderWriter",
 *       "awsRegion": "us-west-2",
 *       "eventSourceARN": "arn:aws:kinesis:us-west-2:515126931066:stream/devRetailStream"
 *     },
 *     {
 *       "kinesis": {
 *         "kinesisSchemaVersion": "1.0",
 *         "partitionKey": "undefined",
 *         "sequenceNumber": "49568749374218235080373793662021150003767486140978823218",
 *         "data": "eyJzY2hlbWEiOiJjb20ubm9yZHN0cm9tL3JldGFpb[...]I3MyIsImNhdGVnb3J5IjoiU3dlYXRlcnMgZm9yIE1lbiJ9fQ==",
 *         "approximateArrivalTimestamp": 1484245766.739
 *       },
 *       "eventSource": "aws:kinesis",
 *       "eventVersion": "1.0",
 *       "eventID": "shardId-000000000003:49568749374218235080373793662021150003767486140978823218",
 *       "eventName": "aws:kinesis:record",
 *       "invokeIdentityArn": "arn:aws:iam::515126931066:role/devProductCatalogReaderWriter",
 *       "awsRegion": "us-west-2",
 *       "eventSourceARN": "arn:aws:kinesis:us-west-2:515126931066:stream/devRetailStream"
 *     }
 *   ]
 * }
 */

/**
 * Processes batched Kinesis events in parallel.
 * Usage: `KinesisHandler(eventSchema, moduleName, transformer)`
 * @param eventSchema The json schema for the events on the Kinesis stream.  (Used to validate kinesis.data in one of the records in the above sample Kinesis batch.)
 * @param moduleName The name by which the module instantiating this instance is referred to in error messages.
 * @param transformer The function that takes the Kinesis record's payload and transforms it to match the eventSchema, using fields from the record.  NB There is no validation of whether the passed function actually produces the correct result.
 * @return {Object} Kinesis Handler instance
 */
function KinesisHandler(eventSchema, moduleName, transformer) {
  if (!eventSchema) {
    throw new Error('A schema for the event data on the Kinesis stream must be provided to this handler.')
  }

  if (transformer && (typeof transformer !== 'function' || transformer.length !== 2)) {
    throw new Error('Transformer must be a function of the form (payload, record) => { . . . return result}')
  }

  // set parameters as configured for the instance and initialize pairs mapping for the instance
  this.eventSchemaId = makeSchemaId(eventSchema)
  this.module = moduleName && (moduleName !== '') ? moduleName : 'unnamed module'
  this.transfomer = transformer
  this.schemaMethodPairs = {}

  // set up validator and add event schema to validator
  this.ajv = new AJV()
  this.ajv.addSchema(eventSchema, this.eventSchemaId)

  /**
   * Registers a schema and method pair.
   * @param schema The json schema for the data that triggers the calling of the provided method.
   * @param method The method to invoke on validating to the given schema, must be of the form (event, callback) => {}.
   */
  this.registerSchemaMethodPair = function (schema, method) {
    const schemaId = makeSchemaId(schema)
    this.ajv.addSchema(schema, schemaId)

    if (typeof method !== 'function' || method.length !== 2) {
      throw new Error('Method must be a function of the form (event, callback) => { . . . }')
    }
    this.schemaMethodPairs[schemaId] = method
  }

  /**
   * Process the given event, reporting failure or success to the given callback
   * @param event The event to validate and process with the appropriate logic
   * @param complete The callback with which to report any errors
   */

  this.processEvent = function (event, complete) {
    if (!event || !event.schema) {
      complete(`${constants.METHOD_PROCESS_EVENT} ${constants.BAD_MSG} event did not specify a schema.`)
    } else if (event.schema !== this.eventSchemaId) {
      complete(`${constants.METHOD_PROCESS_EVENT} ${constants.BAD_MSG} event did not have proper schema.  Observed: '${event.schema}' expected: '${this.eventSchemaId}'`)
    } else if (!this.ajv.validate(this.eventSchemaId, event)) {
      complete(`${constants.METHOD_PROCESS_EVENT} ${constants.BAD_MSG} could not validate event to '${this.eventSchemaId}' schema.  Errors: ${this.ajv.errorsText()}`)
    } else if (Object.keys(this.schemaMethodPairs).indexOf(event.data.schema) > -1) {
      if (!this.ajv.validate(event.data.schema, event.data)) {
        complete(`${constants.METHOD_PROCESS_EVENT} ${constants.BAD_MSG} could not validate event to '${event.data.schema}' schema. Errors: ${this.ajv.errorsText()}`)
      } else {
        this.schemaMethodPairs[event.data.schema](event, complete)
      }
    } else {
      console.log(`${constants.MODULE} for ${this.module} ${constants.METHOD_PROCESS_EVENT} ${constants.BAD_MSG} - event with unsupported data schema (${event.data.schema}) observed.`)
      complete()
    }
  }

  /**
   * @param kinesisEvent The Kinesis event to decode and process.
   * @param context The Lambda context object.
   * @param callback The callback with which to call with results of event processing.
   */

  this.processKinesisEvent = function (kinesisEvent, context, callback) {
    try {
      console.log(`${constants.MODULE} for ${this.module} ${constants.METHOD_PROCESS_KINESIS_EVENT} - Kinesis event received: ${JSON.stringify(kinesisEvent, null, 2)}`)
      if (
        kinesisEvent &&
        kinesisEvent.Records &&
        Array.isArray(kinesisEvent.Records)
      ) {
        let successes = 0
        const complete = (err) => {
          if (err) {
            console.log(err)
            const msg = `${constants.MODULE} for ${this.module} ${err}`
            if (msg.indexOf(`${constants.MODULE} for ${this.module} ${constants.METHOD_PROCESS_EVENT} ${constants.BAD_MSG}`) !== -1) {
              console.log('######################################################################################')
              console.log(msg)
              console.log('######################################################################################')
              successes += 1
            } else {
              throw new Error(msg)
            }
          } else {
            successes += 1
          }
          if (successes === kinesisEvent.Records.length) {
            console.log(`${constants.MODULE} for ${this.module} ${constants.METHOD_PROCESS_KINESIS_EVENT} - all ${kinesisEvent.Records.length} events processed successfully.`)
            callback()
          }
        }
        for (let i = 0; i < kinesisEvent.Records.length; i++) {
          const record = kinesisEvent.Records[i]
          if (
            record.kinesis &&
            record.kinesis.data
          ) {
            let parsed
            try {
              const payload = new Buffer(record.kinesis.data, 'base64').toString()
              console.log(`${constants.MODULE} for ${this.module} ${constants.METHOD_PROCESS_KINESIS_EVENT} - payload: ${payload}`)
              parsed = JSON.parse(payload)
            } catch (ex) {
              complete(`${constants.METHOD_PROCESS_EVENT} ${constants.BAD_MSG} failed to decode and parse the data - "${ex.stack}".`)
            }
            if (parsed) {
              if (this.transfomer) {
                parsed = this.transfomer(parsed, record)
              }
              this.processEvent(parsed, complete)
            }
          } else {
            complete(`${constants.METHOD_PROCESS_EVENT} ${constants.BAD_MSG} record missing Kinesis data.`)
          }
        }
      } else {
        callback(`${constants.MODULE} for ${this.module} ${constants.METHOD_PROCESS_KINESIS_EVENT} - no records received.`)
      }
    } catch (ex) {
      console.log(`${constants.MODULE} for ${this.module} ${constants.METHOD_PROCESS_KINESIS_EVENT} - exception: ${ex.stack}`)
      callback(ex)
    }
  }
}

/**
 * Processes batches of Kinesis events synchronously, maintaining the order within each batch.
 * Usage: `KinesisSynchronousHandler(eventSchema, moduleName, transformer)`
 * @param eventSchema The json schema for the events on the Kinesis stream.  (Used to validate kinesis.data in one of the records in the above sample Kinesis batch.)
 * @param moduleName The name by which the module instantiating this instance is referred to in error messages.
 * @param transformer The function that takes the Kinesis record's payload and transforms it to match the eventSchema, using fields from the record.  NB There is no validation of whether the passed function actually produces the correct result.
 * @return {Object} Kinesis Synchronous Handler instance
 */
function KinesisSynchronousHandler(eventSchema, moduleName, transformer) {
  if (!eventSchema) {
    throw new Error('A schema for the event data on the Kinesis stream must be provided to this handler.')
  }

  if (transformer && (typeof transformer !== 'function' || transformer.length !== 2)) {
    throw new Error('Transformer must be a function of the form (payload, record) => { . . . return result}')
  }

  // set parameters as configured for the instance and initialize pairs mapping for the instance
  this.eventSchemaId = makeSchemaId(eventSchema)
  this.module = moduleName && (moduleName !== '') ? moduleName : 'unnamed module'
  this.transfomer = transformer
  this.schemaMethodPairs = {}
  this.it = null

  // set up validator and add event schema to validator
  this.ajv = new AJV()
  this.ajv.addSchema(eventSchema, this.eventSchemaId)

  /**
   * Registers a schema and method pair.
   * @param schema The json schema for the data that triggers the calling of the provided method.
   * @param method The method to invoke on validating to the given schema, must be of the form (event, complete) => {}.
   */
  this.registerSchemaMethodPair = function (schema, method) {
    const schemaId = makeSchemaId(schema)
    this.ajv.addSchema(schema, schemaId)

    if (typeof method !== 'function' || method.length !== 2) {
      throw new Error('Method must be a function of the form (event, callback) => { . . . }')
    }
    this.schemaMethodPairs[schemaId] = method
  }

  /**
   * Process the given event, passing back any non-fatal (data-quality) errors to the iterator that called it.
   * @param event The event to validate and process with the appropriate logic
   */
  this.processEvent = function (event) {
    const that = this
    // NB we need a setTimeout to defer the call to it.next until *later*, when the generator actually reaches a paused state, after completing the current thread of execution through processEvent
    if (!event || !event.schema) {
      setTimeout(() => that.it.next(`${constants.METHOD_PROCESS_EVENT} ${constants.BAD_MSG} event did not specify a schema.`), 0)
    } else if (event.schema !== this.eventSchemaId) {
      setTimeout(() => that.it.next(`${constants.METHOD_PROCESS_EVENT} ${constants.BAD_MSG} event did not have proper schema.  Observed: '${event.schema}' expected: '${this.eventSchemaId}'`), 0)
    } else if (!this.ajv.validate(this.eventSchemaId, event)) {
      setTimeout(() => that.it.next(`${constants.METHOD_PROCESS_EVENT} ${constants.BAD_MSG} could not validate event to '${this.eventSchemaId}' schema.  Errors: ${this.ajv.errorsText()}`), 0)
    } else if (Object.keys(this.schemaMethodPairs).indexOf(event.data.schema) > -1) {
      if (!this.ajv.validate(event.data.schema, event.data)) {
        setTimeout(() => that.it.next(`${constants.METHOD_PROCESS_EVENT} ${constants.BAD_MSG} could not validate event to '${event.data.schema}' schema. Errors: ${this.ajv.errorsText()}`), 0)
      } else {
        this.schemaMethodPairs[event.data.schema](event, (err) => {
          if (err) {
            console.log('Fatal error while processing. ', err)
            throw new Error(`${constants.MODULE} for ${this.module} ${err}`) // could have the iterator throw this back to yield, but not much point having it there than here, since these are not being handled (except by re-try)
          } else {
            console.log(`${constants.MODULE} for ${this.module} ${constants.METHOD_PROCESS_KINESIS_EVENT} - one ${event.data.schema} event processed successfully.`)
            that.it.next()
          }
        })
      }
    } else {
      setTimeout(() => {
        // This is not a bad message; it's just not anything we care about.
        console.log(`${constants.MODULE} for ${this.module} ${constants.METHOD_PROCESS_EVENT} - skipping event with unsupported data schema (${event.data.schema}).`)
        that.it.next()
      }, 0)
    }
  }

  /**
   * Generates an iterator to synchronously process each record in the batch with the given logic (parse, process, collect bad messages)
   * @param kinesisEvent The batch of events to validate and process with the generated iterator
   */
  this.recordGenerator = function* (kinesisEvent) {
    const badMessages = [] // bad in the sense that they failed for validation or data quality reasons, but not a fatal error that would cause a re-try, such as an error while interacting with an AWS resource
    for (let i = 0; i < kinesisEvent.Records.length; i++) {
      const record = kinesisEvent.Records[i]
      if (
        record.kinesis &&
        record.kinesis.data
      ) {
        let parsed
        try {
          const payload = new Buffer(record.kinesis.data, 'base64').toString()
          console.log(`${constants.MODULE} for ${this.module} ${constants.METHOD_PROCESS_KINESIS_EVENT} - payload: ${payload}`)
          parsed = JSON.parse(payload)
        } catch (ex) {
          badMessages.push(`${constants.METHOD_PROCESS_EVENT} ${constants.BAD_MSG} failed to decode and parse the data - "${ex.stack}".`)
        }
        if (parsed) {
          if (this.transfomer) {
            parsed = this.transfomer(parsed, record)
          }
          const recordError = yield this.processEvent(parsed)
          if (recordError) {
            badMessages.push(recordError)
          }
        }
      } else {
        badMessages.push(`${constants.METHOD_PROCESS_EVENT} ${constants.BAD_MSG} record missing Kinesis data.`)
      }
    }
    console.log('Messages with invalid data: ', badMessages)
  }

  /**
   * @param kinesisEvent The Kinesis event to decode and process.
   * @param context The Lambda context object.
   * @param callback The callback with which to call with results of event processing.
   */
  this.processKinesisEvent = function (kinesisEvent, context, callback) {
    try {
      console.log(`${constants.MODULE} for ${this.module} ${constants.METHOD_PROCESS_KINESIS_EVENT} - Kinesis event received: ${JSON.stringify(kinesisEvent, null, 2)}`) // TODO remove
      if (
        kinesisEvent &&
        kinesisEvent.Records &&
        Array.isArray(kinesisEvent.Records)
      ) {
        this.it = this.recordGenerator(kinesisEvent)
        this.it.next()
      } else {
        callback(`${constants.MODULE} for ${this.module} ${constants.METHOD_PROCESS_KINESIS_EVENT} - no records received.`)
      }
    } catch (ex) {
      console.log(`${constants.MODULE} for ${this.module} ${constants.METHOD_PROCESS_KINESIS_EVENT} - exception: ${ex.stack}`)
      callback(ex)
    }
  }
}

module.exports = {
  KinesisHandler,
  KinesisSynchronousHandler,
}
