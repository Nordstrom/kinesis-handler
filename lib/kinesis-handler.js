'use strict'

const AJV = require('ajv')
const aws = require('aws-sdk')

const constants = {
  // generic reference to self in error messages
  MODULE: 'Kinesis Handler',
  // methods
  METHOD_PROCESS_EVENT: 'processEvent',
  METHOD_PROCESS_KINESIS_EVENT: 'processKinesisEvent',
  // errors
  BAD_MSG: 'bad msg:', // These messages aren't bad, so much as misunderstood (not validating with a known schema).  TODO make class of Errors.
}

function makeSchemaId(schema) {
  return `${schema.self.vendor}/${schema.self.name}/${schema.self.version}`
}

module.exports = KinesisHandler

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
 * Creates Kinesis Handler instance.
 * Usage: `KinesisHandler(schema, parallel)`
 * @param schema The json schema for the events on the Kinesis stream.  (Used to validate kinesis.data in one of the records in the above sample Kinesis batch.)
 * @param synch Default is false.  If true, process batches of Kinesis events synchronously, maintaining the order within each batch.  Otherwise, process batches asynchronously, in parallel.
 * @param moduleName The name by which the module instantiating this instance is referred to in error messages.
 * @return {Object} Kinesis handler instance
 */
function KinesisHandler(schema, synch, moduleName) {
  if (!schema) {
    throw new Error('A schema for the event data on the Kinesis stream must be provided to this handler.')
  }

  // set parameters as configured for the instance and initialize pairs mapping for the instance
  this.eventSchemaId = makeSchemaId(schema)
  this.synch = synch
  this.module = moduleName && (moduleName !== '') ? moduleName : this.module
  this.schemaMethodPairs = {}

  // set up validator and add event schema to validator
  this.ajv = new AJV()
  this.ajv.addSchema(schema, this.eventSchemaId)

  // attach methods directly called by instantiator
  this.registerSchemaMethodPair = registerSchemaMethodPair
  this.handler = processKinesisEvent

  /**
   * Registers a schema and method pair.
   * @param schema The json schema for the data that triggers the calling of the provided method.
   * @param method The method to invoke on validating to the given schema, must be of the form (event, complete) => {}.  TODO enforce this.
   */
  function registerSchemaMethodPair(schema, method) {
    const schemaId = makeSchemaId(schema)
    this.ajv.addSchema(schema, schemaId)
    this.schemaMethodPairs[schemaId] = method
  }

  /**
   * Process the given event, reporting failure or success to the given callback
   * @param event The event to validate and process with the appropriate logic
   * @param complete The callback with which to report any errors
   */
  function processEvent(event, complete) {
    if (!event || !event.schema) {
      complete(`${constants.METHOD_PROCESS_EVENT} ${constants.BAD_MSG} event did not specify a schema.`)
    } else if (event.schema !== this.eventSchemaId) {
      complete(`${constants.METHOD_PROCESS_EVENT} ${constants.BAD_MSG} event did not have proper schema.  Observed: '${event.schema}' expected: '${this.eventSchemaId}'`)
    } else if (!ajv.validate(this.eventSchemaId, event)) {
      complete(`${constants.METHOD_PROCESS_EVENT} ${constants.BAD_MSG} could not validate event to '${this.eventSchemaId}' schema.  Errors: ${ajv.errorsText()}`)
    } else if (Object.keys(this.schemaMethodPairs).indexOf(event.data.schema) > -1) {
      if (!ajv.validate(event.data.schema, event.data)) {
        complete(`${constants.METHOD_PROCESS_EVENT} ${constants.BAD_MSG} could not validate event to '${event.data.schema}' schema. Errors: ${ajv.errorsText()}`)
      } else {
        this.schemaMethodPairs[event.data.schema](event, complete)
      }
    } else {
      // TODO remove console.log and pass the above message once we are only receiving subscribed events
      console.log(`${this.module} ${constants.METHOD_PROCESS_EVENT} ${constants.BAD_MSG} - event with unsupported data schema (${event.data.schema}) observed.`)
      complete()
    }
  }

    /**
     * @param kinesisEvent The Kinesis event to decode and process.
     * @param context The Lambda context object.
     * @param callback The callback with which to call with results of event processing.
     */
    function processKinesisEvent(kinesisEvent, context, callback) {
      if (this.synch) {
        console.log('TODO implement synchronous batch handling.')
      } else {
        try {
          console.log(`${this.module} ${constants.METHOD_PROCESS_KINESIS_EVENT} - Kinesis event received: ${JSON.stringify(kinesisEvent, null, 2)}`)
          if (
            kinesisEvent &&
            kinesisEvent.Records &&
            Array.isArray(kinesisEvent.Records)
          ) {
            let successes = 0
            const complete = (err) => {
              if (err) {
                console.log(err)
                // TODO uncomment following
                // throw new Error(`${this.module} ${err}`);
                // TODO remove rest of block to use above.
                const msg = `${this.module} ${err}`
                if (msg.indexOf(`${this.module} ${constants.METHOD_PROCESS_EVENT} ${constants.BAD_MSG}`) !== -1) {
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
                console.log(`${this.module} ${constants.METHOD_PROCESS_KINESIS_EVENT} - all ${kinesisEvent.Records.length} events processed successfully.`)
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
                  console.log(`${this.module} ${constants.METHOD_PROCESS_KINESIS_EVENT} - payload: ${payload}`)
                  parsed = JSON.parse(payload)
                } catch (ex) {
                  complete(`${constants.METHOD_PROCESS_EVENT} ${constants.BAD_MSG} failed to decode and parse the data - "${ex.stack}".`)
                }
                if (parsed) {
                  impl.processEvent(parsed, complete)
                }
              } else {
                complete(`${constants.METHOD_PROCESS_EVENT} ${constants.BAD_MSG} record missing Kinesis data.`)
              }
            }
          } else {
            callback(`${this.module} ${constants.METHOD_PROCESS_KINESIS_EVENT} - no records received.`)
          }
        } catch (ex) {
          console.log(`${this.module} ${constants.METHOD_PROCESS_KINESIS_EVENT} - exception: ${ex.stack}`)
          callback(ex)
        }
      }
    }
}

