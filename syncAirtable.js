const Airtable = require('airtable')
const config = require('config')
const request = require('request-promise').defaults({ gzip: true, json: true })

const base = new Airtable({ apiKey: config.get('airtableKey') }).base(config.get('baseId'))

function processDatasetList (apis, idx) {
  if (idx === apis.length) {
    return false
  }
  let api = apis[idx]
  return processDatasetAsync(api.url, api.options)
    .then(() => {
      processDatasetList (apis, idx + 1)
    }
  )
}

function processDatasetAsync(url, options) {
  return new Promise(function (resolve, reject) {
    return processDataset(url, options, resolve, reject)
  })
}

function processDataset(url, options, onComplete, onError) {
  return request({
    url: url + options.pageParam + '=' + options.page,
    headers: {
      'Authorization': 'Basic ' + new Buffer(config.get('user') + ':' + config.get('pass')).toString('base64'),
      'X-Socrata-Host': 'data.sfgov.org',
      'X-App-Token': config.get('socrataAppToken')
    }
  })
    .then(body => {
      // for discovery API, returns an object with results as an array
      if (typeof body === 'object' && !Array.isArray(body)) {
        body = body.results
      }
      if (body.length > 0) {
        console.log(`${body.length} records returned from ${url}`)
        body.forEach(record => {
          let payload = options.transform(record)
          findAndUpdate(payload, options.transform)
        })
        options.page = options.page + options.add
        setTimeout(processDataset, 25000, url, options, onComplete, onError)
      } else {
        console.log(`Completed querying ${url}`)
        return onComplete('success')
      }
    }).catch(err => {
      console.error(err)
      return onError('error')
    })
}

function findAndUpdate(payload, transform) {
  console.log('Querying Airtable for dataset ' + payload['ID'])
  base('Data Catalog').select({
    filterByFormula: '{ID} = "' + payload['ID'] + '"',
    maxRecords: 1
  }).firstPage((err, records) => {
    if (err) { console.log('Error on ' + payload['ID']); console.error(err); return; }

    if (records.length === 0) {
      createRecord(payload)
    } else {
      updateRecord(records[0].getId(), payload)
    }
  })
}

function updateRecord(id, payload) {
  base('Data Catalog').update(id, payload, (err, record) => {
    if (err) { console.error(err); return; }
    console.log('Update: ' + record.get('ID'));
  });
}

function createRecord(payload) {
  base('Data Catalog').create(payload, (err, record) => {
    if (err) { console.error(err); return; }
    console.log('Create: ' + record.get('ID'));
  });
}

module.exports = {
  processDataset: processDataset,
  processDatasetList: processDatasetList
}