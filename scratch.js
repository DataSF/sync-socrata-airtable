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
/*
syncDatasetProfiles()

export function syncDiscoveryAPI (url, transform, cb) {
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

export function syncMetadataAPI (cb) {
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

export function syncDictionaryAPI () {
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

export function syncDatasetProfiles () {
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
*/

/*
syncDatasetsAsync('https://api.us.socrata.com/api/catalog/v1?domains=data.sfgov.org&search_context=data.sfgov.org&provenance=official&limit=3&public=true&',
  {
    pageParam: 'offset',
    page: 0,
    add: 3,
    transform: transformDiscoveryPublic
  }).then(() => {
    syncDatasetsAsync('https://api.us.socrata.com/api/catalog/v1?domains=data.sfgov.org&search_context=data.sfgov.org&provenance=official&limit=3&public=false&',
    {
      pageParam: 'offset',
      page: 0,
      add: 3,
      transform: transformDiscoveryPrivate
    })
  }).then(() => {

  })
  */