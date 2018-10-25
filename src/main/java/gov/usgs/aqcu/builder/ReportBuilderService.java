package gov.usgs.aqcu.builder;

import java.math.BigDecimal;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.ControlConditionType;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.FieldVisitDataServiceResponse;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.FieldVisitDescription;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.LocationDescription;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.ParameterMetadata;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.Qualifier;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.TimeSeriesDataServiceResponse;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.TimeSeriesDescription;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.TimeSeriesPoint;

import gov.usgs.aqcu.model.DvHydrographPoint;
import gov.usgs.aqcu.model.DvHydrographReport;
import gov.usgs.aqcu.model.DvHydrographReportMetadata;
import gov.usgs.aqcu.model.FieldVisitMeasurement;
import gov.usgs.aqcu.model.InstantRange;
import gov.usgs.aqcu.model.MinMaxData;
import gov.usgs.aqcu.model.MinMaxPoint;
import gov.usgs.aqcu.model.TimeSeriesCorrectedData;
import gov.usgs.aqcu.model.nwis.GroundWaterParameter;
import gov.usgs.aqcu.model.nwis.ParameterRecord;
import gov.usgs.aqcu.parameter.DvHydrographRequestParameters;
import gov.usgs.aqcu.retrieval.FieldVisitDataService;
import gov.usgs.aqcu.retrieval.FieldVisitDescriptionService;
import gov.usgs.aqcu.retrieval.LocationDescriptionListService;
import gov.usgs.aqcu.retrieval.NwisRaService;
import gov.usgs.aqcu.retrieval.ParameterListService;
import gov.usgs.aqcu.retrieval.QualifierLookupService;
import gov.usgs.aqcu.retrieval.TimeSeriesDataService;
import gov.usgs.aqcu.retrieval.TimeSeriesDescriptionService;
import gov.usgs.aqcu.util.AqcuReportUtils;
import gov.usgs.aqcu.util.AqcuTimeUtils;
import gov.usgs.aqcu.util.BigDecimalSummaryStatistics;
import gov.usgs.aqcu.util.DoubleWithDisplayUtil;
import gov.usgs.aqcu.util.TimeSeriesUtils;

@Service
public class ReportBuilderService {

	protected static final String ESTIMATED_QUALIFIER_VALUE = "ESTIMATED";
	protected static final String VOLUMETRIC_FLOW_UNIT_GROUP_VALUE = "Volumetric Flow";
	private static final String DISCHARGE_PARAMETER = "Discharge";

	private DataGapListBuilderService dataGapListBuilderService;
	private FieldVisitDataService fieldVisitDataService;
	private FieldVisitDescriptionService fieldVisitDescriptionService;
	private LocationDescriptionListService locationDescriptionListService;
	private NwisRaService nwisRaService;
	private ParameterListService parameterListService;
	private QualifierLookupService qualifierLookupService;
	private TimeSeriesDataService timeSeriesDataService;
	private TimeSeriesDescriptionService timeSeriesDescriptionService;

	@Value("${sims.base.url}")
	private String simsUrl;
	@Value("${waterdata.base.url}")
	private String waterdataUrl;

	@Autowired
	public ReportBuilderService(DataGapListBuilderService dataGapListBuilderService,
			FieldVisitDataService fieldVisitDataService, FieldVisitDescriptionService fieldVisitDescriptionService,
			LocationDescriptionListService locationDescriptionListService, NwisRaService nwisRaService,
			ParameterListService parameterListService, QualifierLookupService qualifierLookupService,
			TimeSeriesDataService timeSeriesDataService,
			TimeSeriesDescriptionService timeSeriesDescriptionService) {
		this.dataGapListBuilderService = dataGapListBuilderService;
		this.fieldVisitDataService = fieldVisitDataService;
		this.fieldVisitDescriptionService = fieldVisitDescriptionService;
		this.locationDescriptionListService = locationDescriptionListService;
		this.nwisRaService = nwisRaService;
		this.parameterListService = parameterListService;
		this.qualifierLookupService = qualifierLookupService;
		this.timeSeriesDataService = timeSeriesDataService;
		this.timeSeriesDescriptionService = timeSeriesDescriptionService;
	}

	public DvHydrographReport buildReport(DvHydrographRequestParameters requestParameters, String requestingUser, String title) {
		DvHydrographReport dvHydroReport = new DvHydrographReport();

		Map<String, TimeSeriesDescription> timeSeriesDescriptions = timeSeriesDescriptionService
				.getTimeSeriesDescriptions(requestParameters);
		Map<String, ParameterMetadata> parameterMetadata = parameterListService.getParameterMetadata();

		TimeSeriesDescription primarySeriesDescription = timeSeriesDescriptions.get(requestParameters.getPrimaryTimeseriesIdentifier());
		ZoneOffset primarySeriesZoneOffset = TimeSeriesUtils.getZoneOffset(primarySeriesDescription);
		String primarySeriesParameter = primarySeriesDescription.getParameter().toString();
		GroundWaterParameter primarySeriesGwParam = GroundWaterParameter.getByDisplayName(primarySeriesParameter);

		TimeSeriesDataServiceResponse primarySeriesDataResponse = timeSeriesDataService.get(
			requestParameters.getPrimaryTimeseriesIdentifier(), requestParameters, primarySeriesZoneOffset, 
			TimeSeriesUtils.isDailyTimeSeries(primarySeriesDescription), false, true, null
		);

		dvHydroReport.setReportMetadata(createDvHydroMetadata(requestParameters, timeSeriesDescriptions,
				primarySeriesDescription, primarySeriesDataResponse, requestingUser, primarySeriesGwParam, title));

		dvHydroReport.setPrimarySeriesQualifiers(primarySeriesDataResponse.getQualifiers());
		dvHydroReport.setPrimarySeriesApprovals(primarySeriesDataResponse.getApprovals());

		if (primarySeriesDataResponse.getPoints() != null) {
			dvHydroReport.setMaxMinData(getMinMaxData(primarySeriesDataResponse.getPoints()));
		}

		if (StringUtils.isNotBlank(requestParameters.getFirstStatDerivedIdentifier())) {
			dvHydroReport.setFirstStatDerived(buildTimeSeriesCorrectedData(timeSeriesDescriptions,
					requestParameters.getFirstStatDerivedIdentifier(), requestParameters, parameterMetadata));
		}

		if (StringUtils.isNotBlank(requestParameters.getSecondStatDerivedIdentifier())) {
			dvHydroReport.setSecondStatDerived(buildTimeSeriesCorrectedData(timeSeriesDescriptions,
					requestParameters.getSecondStatDerivedIdentifier(), requestParameters, parameterMetadata));
		}

		if (StringUtils.isNotBlank(requestParameters.getThirdStatDerivedIdentifier())) {
			dvHydroReport.setThirdStatDerived(buildTimeSeriesCorrectedData(timeSeriesDescriptions,
					requestParameters.getThirdStatDerivedIdentifier(), requestParameters, parameterMetadata));
		}

		if (StringUtils.isNotBlank(requestParameters.getFourthStatDerivedIdentifier())) {
			dvHydroReport.setFourthStatDerived(buildTimeSeriesCorrectedData(timeSeriesDescriptions,
					requestParameters.getFourthStatDerivedIdentifier(), requestParameters, parameterMetadata));
		}

		if (StringUtils.isNotBlank(requestParameters.getFirstReferenceIdentifier())) {
			dvHydroReport.setFirstReferenceTimeSeries(buildTimeSeriesCorrectedData(timeSeriesDescriptions,
					requestParameters.getFirstReferenceIdentifier(), requestParameters, parameterMetadata));
		}

		if (StringUtils.isNotBlank(requestParameters.getSecondReferenceIdentifier())) {
			dvHydroReport.setSecondReferenceTimeSeries(buildTimeSeriesCorrectedData(timeSeriesDescriptions,
					requestParameters.getSecondReferenceIdentifier(), requestParameters, parameterMetadata));
		}

		if (StringUtils.isNotBlank(requestParameters.getThirdReferenceIdentifier())) {
			dvHydroReport.setThirdReferenceTimeSeries(buildTimeSeriesCorrectedData(timeSeriesDescriptions,
					requestParameters.getThirdReferenceIdentifier(), requestParameters, parameterMetadata));
		}

		if (StringUtils.isNotBlank(requestParameters.getComparisonTimeseriesIdentifier())) {
			dvHydroReport.setComparisonSeries(buildTimeSeriesCorrectedData(timeSeriesDescriptions,
					requestParameters.getComparisonTimeseriesIdentifier(), requestParameters, parameterMetadata));
		}

		dvHydroReport.setSimsUrl(AqcuReportUtils.getSimsUrl(dvHydroReport.getReportMetadata().getStationId(), simsUrl));
		dvHydroReport.setWaterdataUrl(getWaterdataUrl(dvHydroReport.getReportMetadata().getStationId()));

		if (primarySeriesGwParam != null) {
			if (!requestParameters.isExcludeDiscrete()) {
				dvHydroReport.setGwlevel(nwisRaService.getGwLevels(requestParameters,
						dvHydroReport.getReportMetadata().getStationId(), primarySeriesGwParam, primarySeriesZoneOffset).getRecords());
			}
		} else if (DISCHARGE_PARAMETER.contentEquals(primarySeriesParameter)) {
			dvHydroReport.setFieldVisitMeasurements(getFieldVisitMeasurements(getFieldVisitData(requestParameters,
					dvHydroReport.getReportMetadata().getStationId(), primarySeriesZoneOffset)));
		} else if (!requestParameters.isExcludeDiscrete()) {
			String unit = primarySeriesDescription.getUnit();
			String nwisPcode = getNwisPcode(primarySeriesParameter, unit);
			if (nwisPcode != null) {
				dvHydroReport.setWaterQuality(nwisRaService.getQwData(requestParameters,
						dvHydroReport.getReportMetadata().getStationId(), nwisPcode, primarySeriesZoneOffset));
			}
		}

		return dvHydroReport;
	}

	protected String getNwisPcode(String aqName, String unit) {
		String pcode = null;

		// First find the NWIS name using the nameAliases
		Optional<ParameterRecord> nwisName = nwisRaService.getAqParameterNames().parallelStream()
				.filter(x -> x.getAlias().equals(aqName))
				.findFirst();

		if (nwisName.isPresent()) {
			// then find the pcode using the name and unit
			Optional<ParameterRecord> unitAlias = nwisRaService.getAqParameterUnits().parallelStream()
					.filter(x -> x.getAlias().equals(unit) && x.getName().equals(nwisName.get().getName()))
					.findAny();
			if (unitAlias.isPresent()) {
				pcode = unitAlias.get().getCode();
			}
		}
		return pcode;
	}

	protected TimeSeriesCorrectedData buildTimeSeriesCorrectedData(
			Map<String, TimeSeriesDescription> timeSeriesDescriptions, String timeSeriesIdentifier,
			DvHydrographRequestParameters requestParameters, Map<String, ParameterMetadata> parameterMetadata) {
		TimeSeriesCorrectedData timeSeriesCorrectedData = null;

		if (timeSeriesDescriptions != null && timeSeriesDescriptions.containsKey(timeSeriesIdentifier)) {
			boolean isDaily = TimeSeriesUtils.isDailyTimeSeries(timeSeriesDescriptions.get(timeSeriesIdentifier));
			ZoneOffset zoneOffset = TimeSeriesUtils.getZoneOffset(timeSeriesDescriptions.get(timeSeriesIdentifier));
			TimeSeriesDataServiceResponse timeSeriesDataServiceResponse = timeSeriesDataService
					.get(timeSeriesIdentifier, requestParameters, zoneOffset, isDaily, false, true, null);

			if (timeSeriesDataServiceResponse != null) {
				timeSeriesCorrectedData = createTimeSeriesCorrectedData(timeSeriesDataServiceResponse, isDaily,
						parameterListService.isVolumetricFlow(parameterMetadata, timeSeriesDataServiceResponse.getParameter()), zoneOffset);
			}
		}

		return timeSeriesCorrectedData;
	}

	protected DvHydrographReportMetadata createDvHydroMetadata(DvHydrographRequestParameters requestParameters,
			Map<String, TimeSeriesDescription> timeSeriesDescriptions,
			TimeSeriesDescription primarySeriesDescription,
			TimeSeriesDataServiceResponse primarySeriesDataResponse, String requestingUser,
			GroundWaterParameter gwParam, String title) {
		DvHydrographReportMetadata metadata = new DvHydrographReportMetadata();

		metadata.setExcludeDiscrete(requestParameters.isExcludeDiscrete());
		metadata.setExcludeMinMax(requestParameters.isExcludeMinMax());
		metadata.setExcludeZeroNegative(requestParameters.isExcludeZeroNegative());

		metadata.setTimezone(AqcuTimeUtils.getTimezone(primarySeriesDescription.getUtcOffset()));
		// Repgen just pulls the date for the headings, so we need to be sure and get
		// the "correct" date - it's internal filtering is potentially slightly skewed
		// by this.
		metadata.setStartDate(requestParameters.getStartInstant(ZoneOffset.UTC));
		metadata.setEndDate(requestParameters.getEndInstant(ZoneOffset.UTC));
		metadata.setTitle(title);

		metadata.setPrimarySeriesLabel(primarySeriesDescription.getIdentifier());

		if (timeSeriesDescriptions.containsKey(requestParameters.getFirstStatDerivedIdentifier())) {
			metadata.setFirstStatDerivedLabel(
					timeSeriesDescriptions.get(requestParameters.getFirstStatDerivedIdentifier()).getIdentifier());
		}
		if (timeSeriesDescriptions.containsKey(requestParameters.getSecondStatDerivedIdentifier())) {
			metadata.setSecondStatDerivedLabel(
					timeSeriesDescriptions.get(requestParameters.getSecondStatDerivedIdentifier()).getIdentifier());
		}
		if (timeSeriesDescriptions.containsKey(requestParameters.getThirdStatDerivedIdentifier())) {
			metadata.setThirdStatDerivedLabel(
					timeSeriesDescriptions.get(requestParameters.getThirdStatDerivedIdentifier()).getIdentifier());
		}
		if (timeSeriesDescriptions.containsKey(requestParameters.getFourthStatDerivedIdentifier())) {
			metadata.setFourthStatDerivedLabel(
					timeSeriesDescriptions.get(requestParameters.getFourthStatDerivedIdentifier()).getIdentifier());
		}
		if (timeSeriesDescriptions.containsKey(requestParameters.getFirstReferenceIdentifier())) {
			metadata.setFirstReferenceTimeSeriesLabel(
					timeSeriesDescriptions.get(requestParameters.getFirstReferenceIdentifier()).getIdentifier());
		}
		if (timeSeriesDescriptions.containsKey(requestParameters.getSecondReferenceIdentifier())) {
			metadata.setSecondReferenceTimeSeriesLabel(
					timeSeriesDescriptions.get(requestParameters.getSecondReferenceIdentifier()).getIdentifier());
		}
		if (timeSeriesDescriptions.containsKey(requestParameters.getThirdReferenceIdentifier())) {
			metadata.setThirdReferenceTimeSeriesLabel(
					timeSeriesDescriptions.get(requestParameters.getThirdReferenceIdentifier()).getIdentifier());
		}
		if (timeSeriesDescriptions.containsKey(requestParameters.getComparisonTimeseriesIdentifier())) {
			metadata.setComparisonSeriesLabel(
					timeSeriesDescriptions.get(requestParameters.getComparisonTimeseriesIdentifier()).getIdentifier());
		}

		metadata.setQualifierMetadata(
				qualifierLookupService.getByQualifierList(primarySeriesDataResponse.getQualifiers()));

		LocationDescription locationDescription = locationDescriptionListService.getByLocationIdentifier(
				primarySeriesDescription.getLocationIdentifier());
		metadata.setStationName(locationDescription.getName());
		metadata.setStationId(locationDescription.getIdentifier());

		metadata.setInverted(gwParam != null && gwParam.isInverted());

		return metadata;
	}

	protected List<FieldVisitDataServiceResponse> getFieldVisitData(DvHydrographRequestParameters requestParameters, String locationIdentifier, ZoneOffset zoneOffset) {
		List<FieldVisitDataServiceResponse> result = new ArrayList<>();
		
		for(FieldVisitDescription desc : fieldVisitDescriptionService.getDescriptions(locationIdentifier, zoneOffset, requestParameters)) {
			result.add(fieldVisitDataService.get(desc.getIdentifier()));
		}

		return result;
	}

	protected List<FieldVisitMeasurement> getFieldVisitMeasurements(List<FieldVisitDataServiceResponse> fieldVisitData) {
		List<FieldVisitMeasurement> result = new ArrayList<>();
		for(FieldVisitDataServiceResponse response : fieldVisitData) {
			result.addAll(fieldVisitDataService.extractFieldVisitMeasurements(response, null));
		}
		return result;
	}

	/**
	 * This method should only be called if the timeSeriesDataServiceResponse is not null.
	 */
	protected TimeSeriesCorrectedData createTimeSeriesCorrectedData(
			TimeSeriesDataServiceResponse timeSeriesDataServiceResponse, boolean isDaily, boolean isVolumetricFlow,
			ZoneOffset zoneOffset) {
		TimeSeriesCorrectedData timeSeriesCorrectedData = new TimeSeriesCorrectedData();

		if (timeSeriesDataServiceResponse.getTimeRange() != null) {
			timeSeriesCorrectedData.setStartTime(AqcuTimeUtils
					.getTemporal(timeSeriesDataServiceResponse.getTimeRange().getStartTime(), isDaily, zoneOffset));
			timeSeriesCorrectedData.setEndTime(AqcuTimeUtils
					.getTemporal(timeSeriesDataServiceResponse.getTimeRange().getEndTime(), isDaily, zoneOffset));
		}

		timeSeriesCorrectedData.setUnit(timeSeriesDataServiceResponse.getUnit());
		timeSeriesCorrectedData.setType(timeSeriesDataServiceResponse.getParameter());

		if (timeSeriesDataServiceResponse.getPoints() != null) {
			timeSeriesCorrectedData
					.setPoints(createDvHydroPoints(timeSeriesDataServiceResponse.getPoints(), isDaily, zoneOffset));
		}

		if (timeSeriesDataServiceResponse.getQualifiers() != null) {
			timeSeriesCorrectedData
					.setEstimatedPeriods(getEstimatedPeriods(timeSeriesDataServiceResponse.getQualifiers()));
		}

		timeSeriesCorrectedData.setVolumetricFlow(isVolumetricFlow);

		timeSeriesCorrectedData.setApprovals(timeSeriesDataServiceResponse.getApprovals());
		timeSeriesCorrectedData.setGaps(
				dataGapListBuilderService.buildGapList(timeSeriesDataServiceResponse.getPoints(), isDaily, zoneOffset));
		timeSeriesCorrectedData.setGapTolerances(timeSeriesDataServiceResponse.getGapTolerances());

		return timeSeriesCorrectedData;
	}

	/**
	 * This method should only be called if the timeSeriesPoints list is not null.
	 */
	protected List<DvHydrographPoint> createDvHydroPoints(List<TimeSeriesPoint> timeSeriesPoints,
			boolean isDaily, ZoneOffset zoneOffset) {
		List<DvHydrographPoint> dvPoints = timeSeriesPoints.parallelStream()
				.filter(x -> x.getValue().getNumeric() != null)
				.map(x -> {
					DvHydrographPoint dvPoint = new DvHydrographPoint();
					dvPoint.setTime(AqcuTimeUtils.getTemporal(x.getTimestamp(), isDaily, zoneOffset));
					dvPoint.setValue(DoubleWithDisplayUtil.getRoundedValue(x.getValue()));
					return dvPoint;
				})
				.collect(Collectors.toList());
		return dvPoints;
	}

	/**
	 * This method should only be called if the qualifiers list is not null.
	 */
	protected List<InstantRange> getEstimatedPeriods(List<Qualifier> qualifiers) {
		List<InstantRange> estimatedPeriods = qualifiers.stream()
			.filter(x -> x.getIdentifier().equals(ESTIMATED_QUALIFIER_VALUE))
			.map(x -> {
				InstantRange dateRange = new InstantRange(x.getStartTime(), x.getEndTime());
				return dateRange;
			})
			.collect(Collectors.toList());
		return estimatedPeriods;
	}

	protected String getWaterdataUrl(String stationId) {
		String url = null;
		if (waterdataUrl != null && stationId != null) {
			url = waterdataUrl + "?site_no=" + stationId;
		}
		return url;
	}

	/**
	 * This method should only be called if the timeSeriesPoints list is not null.
	 */
	protected MinMaxData getMinMaxData(List<TimeSeriesPoint> timeSeriesPoints) {
		Map<BigDecimal, List<MinMaxPoint>> minMaxPoints = timeSeriesPoints.parallelStream()
				.map(x -> {
					MinMaxPoint point = new MinMaxPoint(x.getTimestamp().getDateTimeOffset(), DoubleWithDisplayUtil.getRoundedValue(x.getValue()));
					return point;
				})
				.filter(x -> x.getValue() != null)
				.collect(Collectors.groupingByConcurrent(MinMaxPoint::getValue));

		BigDecimalSummaryStatistics stats = minMaxPoints.keySet().parallelStream()
				.collect(BigDecimalSummaryStatistics::new,
						BigDecimalSummaryStatistics::accept,
						BigDecimalSummaryStatistics::combine);

		return new MinMaxData(stats.getMin(), stats.getMax(), minMaxPoints);
	}
}
