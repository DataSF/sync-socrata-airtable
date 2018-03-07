#!/usr/bin/env node
process.env.UV_THREADPOOL_SIZE = 128

const Airtable = require('airtable')
const config = require('config')
const request = require('request-promise').defaults({ gzip: true, json: true })
const soda = require('soda-js')

const base = new Airtable({ apiKey: config.get('airtableKey') }).base(Buffer.from(config.get('baseId'), 'base64'))
// get data inventory
// push to socrata
// TODO: abstract this more to handle a list of airtable bases and tables and socrata destinations
const sodaOpts = {
  'username': config.get('user'),
  'password': Buffer.from(config.get('pass'), 'base64'),
  'apiToken': Buffer.from(config.get('socrataAppToken'), 'base64')
}
const producer = new soda.Producer('data.sfgov.org', sodaOpts)

function pushDatasetInventory() {
  console.log('sync inventory')
  let data = []
  return base('Dataset Inventory').select({
    filterByFormula: 'NOT(OR({Revised Priority} = "Remove",ID = ""))',
    sort: [{ field: "ID", direction: "asc" }]
  }).eachPage(function page(records, fetchNextPage) {
    // This function (`page`) will get called for each page of records.
    records.forEach(function (record) {
      // Easiest to just map these on to the Socrata API keys
      let lagDays = firstValue(record.get('Lag Days'))
      data.push({
        inventory_id: record.get('ID'),
        department_or_division: record.get('Department or Division'),
        dataset_name: record.get('Dataset Name'),
        dataset_description: record.get('Dataset Description'),
        data_classification: record.get('Data Classification'),
        value: record.get('Value'),
        department_priority: record.get('Department Priority'),
        date_added: record.get('Date Added'),
        publishing_status: record.get('Publishing Status'),
        dataset_id: firstValue(record.get('Dataset ID')),
        dataset_link: {
          url: firstValue(record.get('Published URL'))
        },
        status_notes: record.get('Status Notes'),
        first_published: record.get('First Published'),
        date_created_on_platform: firstValue(record.get('Date Created on Platform')),
        category: firstValue(record.get('Category')),
        required_fields_complete: firstValue(record.get('Required Fields Complete')),
        data_dictionary: firstValue(record.get('Data Dictionary or Equivalent')),
        metadata_complete: firstValue(record.get('Metadata Complete')),
        natively_hosted: firstValue(record.get('Natively Hosted')),
        on_time: firstValue(record.get('On Time')),
        publishing_approach: firstValue(record.get('Publishing Approach')),
        automated_by: firstValue(record.get('Automated By')),
        lag: lagDays > 0,
        lag_days: lagDays
      })
    })
    // To fetch the next page of records, call `fetchNextPage`.
    // If there are more records, `page` will get called again.
    // If there are no more records, `done` will get called.
    fetchNextPage()

  }, function done(err) {
    if (err) { console.error(err); return; }
    // truncate dataset - this is simplest way to drop removed inventory records
    producer.operation()
      .withDataset(config.get('inventoryId'))
      .truncate()
      .on('success', row => {
        console.log(row)
        // upsert in bulk
        producer.operation()
          .withDataset(config.get('inventoryId'))
          .upsert(data)
          .on('success', row => { console.log(row); })
          .on('error', err => { console.log('error: ' + err) })
      })
      .on('error', err => { console.log('error: ' + err) })
  })
}

function pushAlertLog() {
  console.log('sync alert log')
  let data = []
  return base('Public Issue Log').select()
    .eachPage(function page(records, fetchNextPage) {
      records.forEach(function(record) {
        data.push({
          alert_date: record.get('Alert Date'),
          alert_details: record.get('Alert Details'),
          dataset_link: firstValue(record.get('Dataset Link')),
          dataset_name: firstValue(record.get('Dataset Name')),
          resolution: record.get('Resolution'),
          resolution_date: record.get('Resolution Date'),
          status: record.get('Status'),
          dataset_id: firstValue(record.get('Dataset ID'))
        })
      })
      fetchNextPage()
    }, function done(err) {
      if (err) { console.error(err); return; }

      producer.operation()
      .withDataset(config.get('alertLogId'))
      .truncate()
      .on('success', row => {
        console.log(row)
        // upsert in bulk
        producer.operation()
          .withDataset(config.get('alertLogId'))
          .upsert(data)
          .on('success', row => { console.log(row); })
          .on('error', err => { console.log('error: ' + err) })
      })
      .on('error', err => { console.log('error: ' + JSON.stringify(err)) })
    })
}

function firstValue(arr) {
  return Array.isArray(arr) ? arr[0] : arr
}

module.exports = {
  pushDatasetInventory: pushDatasetInventory,
  pushAlertLog: pushAlertLog
}