#!/usr/bin/env node
process.env.UV_THREADPOOL_SIZE = 128

const Airtable = require('airtable')
const config = require('config')
const request = require('request-promise').defaults({gzip: true, json: true})

const base = new Airtable({ apiKey: config.get('airtableKey') }).base(config.get('baseId'))


// To capture public vs. private, we must query the Discovery API using the public param twice

/*
syncDiscoveryAPI(
  'https://api.us.socrata.com/api/catalog/v1?domains=data.sfgov.org&search_context=data.sfgov.org&provenance=official&limit=100&public=true&', transformDiscoveryPublic,
  () => {
    syncDiscoveryAPI(
      'https://api.us.socrata.com/api/catalog/v1?domains=data.sfgov.org&search_context=data.sfgov.org&provenance=official&limit=100&public=false&', transformDiscoveryPrivate, 
      () => {
        syncMetadataAPI(syncDictionaryAPI)
      })
})
*/

syncDatasetProfiles()

function syncDiscoveryAPI (url, transform, cb) {
  syncDatasets(
    url,
    {
      pageParam: 'offset',
      page: 0,
      add: 100,
      transform: transform
    },
    cb 
  )
}

function syncMetadataAPI (cb) {
  syncDatasets('https://data.sfgov.org/api/views/metadata/v1?limit=100&', 
  {
    pageParam: 'page',
    page: 1,
    add: 1,
    transform: transformMetadata
  }, () => {
    cb()
  })
}

function syncDictionaryAPI () {
  syncDatasets('https://data.sfgov.org/resource/cq5k-ka7d.json?$select=datasetid,SUM(CASE(field_documented=true,1)),SUM(CASE(field_documented=false,0)),count(*)&$group=datasetid&$limit=100&$order=datasetid&',
  {
    pageParam: '$offset',
    page: 0,
    add: 100,
    transform: transformDocumentation
  }, () => {
    console.log('Done loading assets from master data dictionary')
    syncDatasetProfiles()
  })
}

function syncDatasetProfiles () {
  syncDatasets('https://data.sfgov.org/resource/8ez2-fksg.json?&$limit=100&$order=datasetid&',
{
  pageParam: '$offset',
  page: 0,
  add: 100,
  transform: transformProfiles
}, () => {
  console.log('Done loading assets from asset inventory')
  })
}

function syncDatasets(url, options, cb) {
  request({
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
      console.log(body.length + ' records returned from Socrata API call')
      body.forEach(record => {
        let payload = options.transform(record)
        findAndUpdate(payload, options.transform)
      })
      options.page = options.page + options.add
      setTimeout(syncDatasets, 45000, url, options, cb)
    } else {
      cb()
    }
  }).catch(err => {
    console.error(err)
  })
}

function findAndUpdate (payload, transform) {
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

function updateRecord (id, payload) {
  base('Data Catalog').update(id, payload, (err, record) => {
      if (err) { console.error(err); return; }
      console.log('Update: ' + record.get('ID'));
  });
}

function createRecord (payload) {
  base('Data Catalog').create(payload, (err, record) => {
      if (err) { console.error(err); return; }
      console.log('Create: ' + record.get('ID'));
  });
}

function transformMetadata (record) {
  return {
    'ID': record.id,
    'Name': record.name,
    'Description': record.description,
    'URL': record.webUri,
    'Creation Date': record.createdAt,
    'Data Updated Date': record.dataUpdatedAt,
    'Metadata Updated Date': record.metadataUpdatedAt,
    'Updated Date': record.updatedAt,
    'Category': record.category,
    'Keywords': record.tags ? record.tags.join(', ') : null,
    'License': record.license,
    'Public': !record.hideFromCatalog,
    'Geographic unit': record.customFields && record.customFields['Detailed Descriptive'] ? record.customFields['Detailed Descriptive']['Geographic unit'] : null,
    'Publishing Department': record.customFields && record.customFields['Department Metrics'] ? record.customFields['Department Metrics']['Publishing Department'] : null,
    'Data change frequency': record.customFields && record.customFields['Publishing Details'] ? record.customFields['Publishing Details']['Data change frequency'] : null,
    'Publishing frequency': record.customFields && record.customFields['Publishing Details'] ? record.customFields['Publishing Details']['Publishing frequency'] : null,
    'Provenance': record.provenance.toLowerCase()
  }
}

function transformDiscoveryPrivate (record) {
  record.resource.public = false
  return transformDiscovery(record)
}

function transformDiscoveryPublic (record) {
  record.resource.public = true
  return transformDiscovery(record)
}

function transformDiscovery (record) {
  return {
    'ID': record.resource.id,
    'Name': record.resource.name,
    'Description': record.resource.description,
    'Type': record.resource.type,
    'Updated Date': record.resource.updatedAt,
    'Creation Date': record.resource.createdAt,
    'Owner': record.owner.display_name || null,
    'Parent': record.resource.parent_fxf ? record.resource.parent_fxf.join(', ') : null,
    'Provenance': record.resource.provenance,
    'Public': record.resource.public || false,
    'URL': record.permalink,
    'License': record.metadata.license || null,
    'Category': record.classification.domain_category || null
  }
}

function transformDocumentation (record) {
  let numFieldsDoc = record.SUM_CASE_field_documented_TRUE_1 || 0
  let percDoc = (parseInt(numFieldsDoc, 10) / parseInt(record.count))

  return {
    'ID': record.datasetid,
    'Number of Fields': parseInt(record.count, 10),
    'Percent Documented from Field Profiles': percDoc * 100,
  }
}

function transformProfiles (record) {
  return {
    'ID': record.datasetid,
    'Data Updated Date': record.last_updt_dt_data,
    'Number of Fields': parseInt(record.field_count, 10),
    'Number of Documented Fields': parseInt(record.documented_count),
    'Percent Documented': parseFloat(record.documented_percentage) * 100
  }
}