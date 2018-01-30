#!/usr/bin/env node
process.env.UV_THREADPOOL_SIZE = 128

const Airtable = require('airtable')
const config = require('config')
const request = require('request-promise').defaults({gzip: true, json: true})
const soda = require('soda-js')

const base = new Airtable({ apiKey: config.get('airtableKey') }).base(config.get('baseId'))

// get data inventory
// push to socrata

const sodaOpts = {
  'username': config.get('user'),
  'password': config.get('pass'),
  'apiToken': config.get('socrataAppToken')
}
const producer = new soda.Producer('data.sfgov.org', sodaOpts)
let data = []
base('Dataset Inventory').select({
  filterByFormula: 'NOT(OR({Revised Priority} = "Remove",ID = ""))',
  sort: [{field: "ID", direction: "asc"}]
}).eachPage(function page(records, fetchNextPage) {
  // This function (`page`) will get called for each page of records.
  records.forEach(function(record) {
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
          .on('success', row => { console.log(row)})
          .on('error', err => { console.log('error: ' + err)})
      })
      .on('error', err => { console.log('error: ' + err)})
})

function firstValue(arr) {
  return Array.isArray(arr) ? arr[0] : arr
}