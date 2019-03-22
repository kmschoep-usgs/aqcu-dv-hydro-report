# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html). (Patch version X.Y.0 is implied if not specified.)

## [Unreleased]
### Added
- debug logging
- log execution time of report
- travis.yml for jacoco and coveralls reporting
- jacoco and coveralls plugin to pom, excluding config and spring-specific classes from coveralls reporting

### Changed
- update framework version to 0.0.6-SNAPSHOT

## [0.0.6] - 2019-02-20
### Added
- specific timeout values
- default Aquarius timeout.

### Changed
- Updated to framework release 0.0.5
- Disabled TLS 1.0/1.1 by default.
- update SDK to 18.8.1 

## [0.0.5] - 2018-11-02
### Added
- DVHydroController
- FiveYearController
- FiveYearRequestParameters
- Five Year GW Summary report as a sub-report of DV Hydro

### Changed
- Updated to framework release 0.0.4. 

### Removed
- Removed services and models that were added to the framework from UV Hydro
- WaterLevelRecordDeserializer
- WaterLevelRecordsDeserializer
- WaterQualitySampleRecordDeserializer
- NwisRaClient
- FieldVisitMeasurement
- InstantRange
- MeasurementGrade
- MinMaxData
- MinMaxPoint
- WaterLevelRecord
- WaterQualitySampleRecord
- WqValue
- GroundWaterParameter
- NwisRaTimeZones
- ParameterRecord	
- ParameterRecords
- WaterLevelRecords
- WaterQualitySampleRecords
- FieldVisitDataService
- FieldVisitDescriptionService
- NwisRaService
- ParameterListService
- TimeSeriesDataCorrectedService

## [0.0.4] - 2018-08-31
### Changed
- Update to AQ SDK 18.6.2
- Update to framework 0.0.3-SNAPSHOT. 
- Update to aqcu-framework 0.0.3 release. 

## [0.0.3] - 2018-07-17
### Added
- Oauth2 request interceptor to forward auth to NWIS-RA. 

## [0.0.2] - 2018-07-13
### Changed
- add publish data to field visit measurements


## [0.0.1] - 2018-06-15
### Added
- Initial release - happy path.
- ReportBuilderService
- NwisRaClient
- WaterLevelRecordDeserializer
- WaterLevelRecordsDeserializer
- WaterQualitySampleRecordDeserializer
- DvHydrographPoint
- DvHydrographReport
- DvHydrographReportMetadata
- FieldVisitMeasurement
- InstantRange
- MeasurementGrade
- MinMaxData
- MinMaxPoint
- TimeSeriesCorrectedData
- WaterLevelRecord
- WaterQualitySampleRecord
- WqValue
- FieldVisitDataService
- FieldVisitDescriptionService
- NwisRaService
- ParameterListService
- TimeSeriesDataCorrectedService
- TimeSeriesDescriptionService
- CidaAuthAuthenticationToken
- CidaAuthRequestInterceptor
- CidaAuthTokenRelayFilter
- CidaAuthTokenSecurityFilter
- BigDecimalSummaryStatistics
- DoubleWithDisplayUtil
- SwaggerGsonSerializer
- SecurityConfig
- SwaggerConfig
- WebMvcConfig
- DvHydrographRequestParameters 	
- StatDerivedIdentifierPresent
- StatDerivedIdentifierPresentValidator
- Application
- Controller
- GroundWaterParameter
- NwisRaTimeZones
- ParameterRecord	
- ParameterRecords
- WaterLevelRecords
- WaterQualitySampleRecords

[Unreleased]: https://github.com/USGS-CIDA/aqcu-dv-hydro-report/compare/aqcu-dv-hydro-report-0.0.6...master
[0.0.6]: https://github.com/USGS-CIDA/aqcu-dv-hydro-report/compare/aqcu-dv-hydro-report-0.0.5...aqcu-dv-hydro-report-0.0.6
[0.0.5]: https://github.com/USGS-CIDA/aqcu-dv-hydro-report/compare/aqcu-dv-hydro-report-0.0.4...aqcu-dv-hydro-report-0.0.5
[0.0.4]: https://github.com/USGS-CIDA/aqcu-dv-hydro-report/compare/aqcu-dv-hydro-report-0.0.3...aqcu-dv-hydro-report-0.0.4
[0.0.3]: https://github.com/USGS-CIDA/aqcu-dv-hydro-report/compare/aqcu-dv-hydro-report-0.0.2...aqcu-dv-hydro-report-0.0.3
[0.0.2]: https://github.com/USGS-CIDA/aqcu-dv-hydro-report/compare/aqcu-dv-hydro-report-0.0.1...aqcu-dv-hydro-report-0.0.2