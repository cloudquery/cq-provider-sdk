# Integration Tests

## Description
Integration tests use terraform to deploy resources.
Every resource terraform file should be described in ./resources/testData and have the same name as tested resource(<provider>_<domain>_<resource_name>). Example: `aws_iam_users.tf` 
Testing routine copies this file among with default *.tf files to a separate folder,
sets default variables prefix=<resource_name> suffix=<machine_hostname>,
deploys resources,
fetches data from provider to database,
queries data using `Filter` field or using default filter based on tags,
compares the received data with expected values.

## Run
To run integration tests you need:
- terraform executable in $PATH
- provider credentials configured via config files or env variables
- sql database deployed  
  to run the tests use command below in PROVIDER root dir:
  ```shell
   go test -v -p 20 ./resources --tags=integration
  ```
  Some tests marked with `integration_skip` tag. They are not ready yet

## Debugging 

For debugging, you can set env variable `TF_NO_DESTROY=true` to leave the directory and resources after the test.
