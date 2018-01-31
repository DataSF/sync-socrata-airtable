#!/usr/bin/env node
process.env.UV_THREADPOOL_SIZE = 128

const syncAirtable = require('./syncAirtable')
const syncSocrata = require('./syncSocrata')

// 1. Define array of API calls

let apiCalls = [
//Discover API, public assets
{
  url: 'https://api.us.socrata.com/api/catalog/v1?domains=data.sfgov.org&search_context=data.sfgov.org&provenance=official&limit=100&public=true&',
  options: {
    pageParam: 'offset',
    page: 0,
    add: 100,
    transform: transformDiscoveryPublic
  }
},
// Discovery API, private assets
{
  url: 'https://api.us.socrata.com/api/catalog/v1?domains=data.sfgov.org&search_context=data.sfgov.org&provenance=official&limit=100&public=false&',
  options: {
    pageParam: 'offset',
    page: 0,
    add: 100,
    transform: transformDiscoveryPrivate
  }
},
// Metadata API
{
  url: 'https://data.sfgov.org/api/views/metadata/v1?limit=100&',
  options: {
    pageParam: 'page',
    page: 1,
    add: 1,
    transform: transformMetadata
  }
},
// Dictionary API
{
  url: 'https://data.sfgov.org/resource/cq5k-ka7d.json?$select=datasetid,SUM(CASE(field_documented=true,1)),SUM(CASE(field_documented=false,0)),count(*)&$group=datasetid&$limit=100&$order=datasetid&',
  options: {
    pageParam: '$offset',
    page: 0,
    add: 100,
    transform: transformDocumentation
  }
},
// Dataset profiles
{
  url: 'https://data.sfgov.org/resource/8ez2-fksg.json?&$limit=100&$order=datasetid&',
  options: {
    pageParam: '$offset',
    page: 0,
    add: 100,
    transform: transformProfiles
  }
}]

// 2. Define transforms from API to Airtable, each accepts a record and maps to a json object schema
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

// 3. Process the list and sync inventory to socrata on completion
syncAirtable.processDatasetList(apiCalls, 0)
  .then(() => {
    syncSocrata.pushDatasetInventory()
  })