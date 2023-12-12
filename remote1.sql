CREATE OR REPLACE FUNCTION `demonstration.translate`(text STRING) RETURNS STRING
REMOTE WITH CONNECTION `us.demonstration`
OPTIONS (
  endpoint = 'ENDPOINT_HERE'
)