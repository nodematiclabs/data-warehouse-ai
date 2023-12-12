CREATE OR REPLACE FUNCTION `demonstration.llm`(prompt STRING) RETURNS STRING
REMOTE WITH CONNECTION `us.demonstration`
OPTIONS (
  endpoint = 'ENDPOINT_HERE',
  max_batching_rows = 10
)