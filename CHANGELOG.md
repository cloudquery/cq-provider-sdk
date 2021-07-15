# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

* Added a changelog :)
* Added support for user defined Primary Keys in [#41](https://github.com/cloudquery/cq-provider-sdk/pull/41)
* Added support to disable delete of data [#41](https://github.com/cloudquery/cq-provider-sdk/pull/41)
* Added meta field, this field holds when row updated last. [#41](https://github.com/cloudquery/cq-provider-sdk/pull/41)


  
  methods returning `(v, ok)` shaped values to support Prisma Go client. <br />
  By [@steebchen](https://github.com/steebchen) in [#1449](https://github.com/99designs/gqlgen/pull/1449)

### Changed
* Changed default insert in provider from Insert to Copy-From, this method improved insert performance [#48](https://github.com/cloudquery/cq-provider-sdk/pull/48)
* **Breaking Change**: default CloudQuery "id" from `id` to `cq_id` [#41](https://github.com/cloudquery/cq-provider-sdk/pull/41)

## [0.2.8] - 2020-07-15

Base version at which changelog was introduced.
