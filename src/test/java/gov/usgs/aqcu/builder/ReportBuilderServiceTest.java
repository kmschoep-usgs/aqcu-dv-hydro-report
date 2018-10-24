package gov.usgs.aqcu.builder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doReturn;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;

import com.aquaticinformatics.aquarius.sdk.timeseries.serializers.InstantDeserializer;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.Approval;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.DischargeActivity;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.DischargeSummary;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.DoubleWithDisplay;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.FieldVisitDataServiceResponse;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.FieldVisitDescription;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.GapTolerance;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.LocationDescription;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.MeasurementGradeType;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.ParameterMetadata;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.Qualifier;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.QualifierMetadata;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.QuantityWithDisplay;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.StatisticalDateTimeOffset;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.StatisticalTimeRange;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.TimeSeriesDataServiceResponse;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.TimeSeriesDescription;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.TimeSeriesPoint;

import gov.usgs.aqcu.ObjectCompare;
import gov.usgs.aqcu.model.DataGap;
import gov.usgs.aqcu.model.DvHydrographPoint;
import gov.usgs.aqcu.model.DvHydrographReport;
import gov.usgs.aqcu.model.DvHydrographReportMetadata;
import gov.usgs.aqcu.model.FieldVisitMeasurement;
import gov.usgs.aqcu.model.InstantRange;
import gov.usgs.aqcu.model.MinMaxData;
import gov.usgs.aqcu.model.MinMaxPoint;
import gov.usgs.aqcu.model.TimeSeriesCorrectedData;
import gov.usgs.aqcu.model.nwis.WaterLevelRecord;
import gov.usgs.aqcu.model.nwis.WaterLevelRecords;
import gov.usgs.aqcu.model.nwis.WaterQualitySampleRecord;
import gov.usgs.aqcu.model.nwis.WqValue;
import gov.usgs.aqcu.model.nwis.GroundWaterParameter;
import gov.usgs.aqcu.model.nwis.ParameterRecord;
import gov.usgs.aqcu.parameter.DvHydrographRequestParameters;
import gov.usgs.aqcu.retrieval.AquariusRetrievalService;
import gov.usgs.aqcu.retrieval.FieldVisitDataService;
import gov.usgs.aqcu.retrieval.FieldVisitDescriptionService;
import gov.usgs.aqcu.retrieval.LocationDescriptionListService;
import gov.usgs.aqcu.retrieval.NwisRaService;
import gov.usgs.aqcu.retrieval.ParameterListService;
import gov.usgs.aqcu.retrieval.QualifierLookupService;
import gov.usgs.aqcu.retrieval.RatingModelInputValuesService;
import gov.usgs.aqcu.retrieval.TimeSeriesDataService;
import gov.usgs.aqcu.retrieval.TimeSeriesDescriptionService;

@RunWith(SpringRunner.class)
public class ReportBuilderServiceTest {
	public static final Instant REPORT_END_INSTANT = Instant.parse("2018-03-17T23:59:59.999999999Z");
	public static final Instant REPORT_START_INSTANT = Instant.parse("2018-03-16T00:00:00.00Z");
	public static final LocalDate REPORT_END_DATE = LocalDate.of(2018, 03, 17);
	public static final LocalDate REPORT_START_DATE = LocalDate.of(2018, 03, 16);
	public static final QualifierMetadata QUALIFIER_METADATA_A = new QualifierMetadata().setIdentifier("a");
	public static final QualifierMetadata QUALIFIER_METADATA_B = new QualifierMetadata().setIdentifier("b");
	public static final QualifierMetadata QUALIFIER_METADATA_C = new QualifierMetadata().setIdentifier("c");
	public static final QualifierMetadata QUALIFIER_METADATA_D = new QualifierMetadata().setIdentifier("d");

	@MockBean
	private AquariusRetrievalService aquariusService;
	@MockBean
	private RatingModelInputValuesService ratingInputService;
	@MockBean
	private DataGapListBuilderService dataGapListBuilderService;
	@SpyBean
	private FieldVisitDataService fieldVisitDataService;
	@MockBean
	private FieldVisitDescriptionService fieldVisitDescriptionService;
	@MockBean
	private LocationDescriptionListService locationDescriptionListService;
	@MockBean
	private NwisRaService nwisRaService;
	@MockBean
	private ParameterListService parameterListService;
	@MockBean
	private QualifierLookupService qualifierLookupService;
	@MockBean
	private TimeSeriesDataService timeSeriesDataService;
	@MockBean
	private TimeSeriesDescriptionService timeSeriesDescriptionService;

	private ReportBuilderService service;
	private Map<String, QualifierMetadata> metadataMap;
	private Instant nowInstant;
	private LocalDate nowLocalDate;

	@Before
	@SuppressWarnings("unchecked")
	public void setup() {
		fieldVisitDataService = Mockito.spy(new FieldVisitDataService(aquariusService, ratingInputService));
		service = new ReportBuilderService(dataGapListBuilderService, fieldVisitDataService,
				fieldVisitDescriptionService, locationDescriptionListService, nwisRaService, parameterListService,
				qualifierLookupService, timeSeriesDataService, timeSeriesDescriptionService);
		metadataMap = buildQualifierMetadata();
		nowInstant = Instant.now();
		nowLocalDate = LocalDate.now();

		// Setup non-mock methods
		when(parameterListService.isVolumetricFlow(any(Map.class), any(String.class))).thenCallRealMethod();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void buildReportNoNwisRaTest() {
		given(parameterListService.getParameterMetadata()).willReturn(getParameterMetadata());
		given(timeSeriesDescriptionService.getTimeSeriesDescriptions(any(DvHydrographRequestParameters.class)))
				.willReturn(buildTimeSeriesDescriptions());
		given(timeSeriesDataService.get(anyString(), any(DvHydrographRequestParameters.class), any(ZoneOffset.class), any(boolean.class), any(boolean.class), any(boolean.class), eq(null)))
				.willReturn(getTimeSeriesDataServiceResponse(true, ZoneOffset.of("-4"), true),
						getTimeSeriesDataServiceResponse(false, ZoneOffset.of("-4"), true));
		given(locationDescriptionListService.getByLocationIdentifier(anyString()))
			.willReturn(new LocationDescription().setIdentifier("0010010000").setName("monitoringLocation"));
		given(dataGapListBuilderService.buildGapList(anyList(), any(boolean.class), any(ZoneOffset.class)))
			.willReturn(getGapList());
		given(qualifierLookupService.getByQualifierList(anyList())).willReturn(metadataMap);
		given(fieldVisitDescriptionService.getDescriptions(anyString(), any(ZoneOffset.class), any(DvHydrographRequestParameters.class)))
			.willReturn(getFieldVisitDecriptions());
		doReturn(getFieldVisitDataServiceResponse(getActivities())).when(fieldVisitDataService).get(anyString());

		DvHydrographReport actual = service.buildReport(buildRequestParameters(), "requestingUser", "DV Hydrograph");
		ObjectCompare.compare(buildExpectedDvHydrographReport(), actual);

		verify(parameterListService).getParameterMetadata();
		verify(timeSeriesDescriptionService).getTimeSeriesDescriptions(any(DvHydrographRequestParameters.class));
		verify(timeSeriesDataService, times(9)).get(anyString(), any(DvHydrographRequestParameters.class), any(ZoneOffset.class), any(boolean.class), any(boolean.class), any(boolean.class), eq(null));
		verify(locationDescriptionListService).getByLocationIdentifier(anyString());
		verify(dataGapListBuilderService, times(8)).buildGapList(anyList(), any(boolean.class), any(ZoneOffset.class));
		verify(qualifierLookupService).getByQualifierList(anyList());
		verify(fieldVisitDataService, times(2)).get(anyString());
		verify(fieldVisitDescriptionService).getDescriptions(anyString(), any(ZoneOffset.class), any(DvHydrographRequestParameters.class));

		verify(nwisRaService, never()).getGwLevels(any(DvHydrographRequestParameters.class), anyString(), any(GroundWaterParameter.class), any(ZoneOffset.class));
		verify(nwisRaService, never()).getQwData(any(DvHydrographRequestParameters.class), anyString(), anyString(), any(ZoneOffset.class));
		verify(nwisRaService, never()).getAqParameterNames();
		verify(nwisRaService, never()).getAqParameterUnits();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void buildGwExcludedReportTest() {
		given(parameterListService.getParameterMetadata()).willReturn(getParameterMetadata());
		given(timeSeriesDescriptionService.getTimeSeriesDescriptions(any(DvHydrographRequestParameters.class)))
				.willReturn(buildGwTimeSeriesDescriptions());
		given(timeSeriesDataService.get(anyString(), any(DvHydrographRequestParameters.class), any(ZoneOffset.class), any(boolean.class), any(boolean.class), any(boolean.class), eq(null)))
				.willReturn(getTimeSeriesDataServiceResponse(true, ZoneOffset.of("-4"), true),
						getTimeSeriesDataServiceResponse(false, ZoneOffset.of("-4"), true));
		given(locationDescriptionListService.getByLocationIdentifier(anyString()))
			.willReturn(new LocationDescription().setIdentifier("0010010000").setName("monitoringLocation"));
		given(dataGapListBuilderService.buildGapList(anyList(), any(boolean.class), any(ZoneOffset.class)))
			.willReturn(getGapList());
		given(qualifierLookupService.getByQualifierList(anyList())).willReturn(metadataMap);

		DvHydrographRequestParameters requestParameters = new DvHydrographRequestParameters();
		requestParameters.setPrimaryTimeseriesIdentifier("a");
		requestParameters.setFirstStatDerivedIdentifier("b");
		requestParameters.setStartDate(REPORT_START_DATE);
		requestParameters.setEndDate(REPORT_END_DATE);
		requestParameters.setExcludeDiscrete(true);

		DvHydrographReport actual = service.buildReport(requestParameters, "requestingUser", "DV Hydrograph");
		ObjectCompare.compare(buildExpectedGwDvHydrographReport(true), actual);

		verify(parameterListService).getParameterMetadata();
		verify(timeSeriesDescriptionService).getTimeSeriesDescriptions(any(DvHydrographRequestParameters.class));
		verify(timeSeriesDataService, times(2)).get(anyString(), any(DvHydrographRequestParameters.class), any(ZoneOffset.class), any(boolean.class), any(boolean.class), any(boolean.class), eq(null));
		verify(locationDescriptionListService).getByLocationIdentifier(anyString());
		verify(dataGapListBuilderService, times(1)).buildGapList(anyList(), any(boolean.class), any(ZoneOffset.class));
		verify(qualifierLookupService).getByQualifierList(anyList());
		verify(fieldVisitDataService, never()).get(anyString());
		verify(fieldVisitDescriptionService, never()).getDescriptions(anyString(), any(ZoneOffset.class), any(DvHydrographRequestParameters.class));

		verify(nwisRaService, never()).getGwLevels(any(DvHydrographRequestParameters.class), anyString(), any(GroundWaterParameter.class), any(ZoneOffset.class));
		verify(nwisRaService, never()).getQwData(any(DvHydrographRequestParameters.class), anyString(), anyString(), any(ZoneOffset.class));
		verify(nwisRaService, never()).getAqParameterNames();
		verify(nwisRaService, never()).getAqParameterUnits();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void buildGwReportTest() {
		given(parameterListService.getParameterMetadata()).willReturn(getParameterMetadata());
		given(timeSeriesDescriptionService.getTimeSeriesDescriptions(any(DvHydrographRequestParameters.class)))
				.willReturn(buildGwTimeSeriesDescriptions());
		given(timeSeriesDataService.get(anyString(), any(DvHydrographRequestParameters.class), any(ZoneOffset.class), any(boolean.class), any(boolean.class), any(boolean.class), eq(null)))
				.willReturn(getTimeSeriesDataServiceResponse(true, ZoneOffset.of("-4"), true),
						getTimeSeriesDataServiceResponse(false, ZoneOffset.of("-4"), true));
		given(locationDescriptionListService.getByLocationIdentifier(anyString()))
			.willReturn(new LocationDescription().setIdentifier("0010010000").setName("monitoringLocation"));
		given(dataGapListBuilderService.buildGapList(anyList(), any(boolean.class), any(ZoneOffset.class)))
			.willReturn(getGapList());
		given(qualifierLookupService.getByQualifierList(anyList())).willReturn(metadataMap);
		given(nwisRaService.getGwLevels(any(DvHydrographRequestParameters.class), anyString(), any(GroundWaterParameter.class), any(ZoneOffset.class)))
			.willReturn(getGwLevels());

		DvHydrographRequestParameters requestParameters = new DvHydrographRequestParameters();
		requestParameters.setPrimaryTimeseriesIdentifier("a");
		requestParameters.setFirstStatDerivedIdentifier("b");
		requestParameters.setStartDate(REPORT_START_DATE);
		requestParameters.setEndDate(REPORT_END_DATE);
		requestParameters.setExcludeDiscrete(false);

		DvHydrographReport actual = service.buildReport(requestParameters, "requestingUser", "DV Hydrograph");
		ObjectCompare.compare(buildExpectedGwDvHydrographReport(false), actual);

		verify(parameterListService).getParameterMetadata();
		verify(timeSeriesDescriptionService).getTimeSeriesDescriptions(any(DvHydrographRequestParameters.class));
		verify(timeSeriesDataService, times(2)).get(anyString(), any(DvHydrographRequestParameters.class), any(ZoneOffset.class), any(boolean.class), any(boolean.class), any(boolean.class), eq(null));
		verify(locationDescriptionListService).getByLocationIdentifier(anyString());
		verify(dataGapListBuilderService, times(1)).buildGapList(anyList(), any(boolean.class), any(ZoneOffset.class));
		verify(qualifierLookupService).getByQualifierList(anyList());
		verify(fieldVisitDataService, never()).get(anyString());
		verify(fieldVisitDescriptionService, never()).getDescriptions(anyString(), any(ZoneOffset.class), any(DvHydrographRequestParameters.class));

		verify(nwisRaService).getGwLevels(any(DvHydrographRequestParameters.class), anyString(), any(GroundWaterParameter.class), any(ZoneOffset.class));
		verify(nwisRaService, never()).getQwData(any(DvHydrographRequestParameters.class), anyString(), anyString(), any(ZoneOffset.class));
		verify(nwisRaService, never()).getAqParameterNames();
		verify(nwisRaService, never()).getAqParameterUnits();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void buildWqExcludedReportTest() {
		given(parameterListService.getParameterMetadata()).willReturn(getParameterMetadata());
		given(timeSeriesDescriptionService.getTimeSeriesDescriptions(any(DvHydrographRequestParameters.class)))
				.willReturn(buildWqTimeSeriesDescriptions("aqname"));
		given(timeSeriesDataService.get(anyString(), any(DvHydrographRequestParameters.class), any(ZoneOffset.class), any(boolean.class), any(boolean.class), any(boolean.class), eq(null)))
				.willReturn(getTimeSeriesDataServiceResponse(true, ZoneOffset.of("-4"), false),
						getTimeSeriesDataServiceResponse(false, ZoneOffset.of("-4"), false));
		given(locationDescriptionListService.getByLocationIdentifier(anyString()))
			.willReturn(new LocationDescription().setIdentifier("0010010000").setName("monitoringLocation"));
		given(dataGapListBuilderService.buildGapList(anyList(), any(boolean.class), any(ZoneOffset.class)))
			.willReturn(getGapList());
		given(qualifierLookupService.getByQualifierList(anyList())).willReturn(metadataMap);

		DvHydrographRequestParameters requestParameters = new DvHydrographRequestParameters();
		requestParameters.setPrimaryTimeseriesIdentifier("a");
		requestParameters.setSecondStatDerivedIdentifier("c");
		requestParameters.setStartDate(REPORT_START_DATE);
		requestParameters.setEndDate(REPORT_END_DATE);
		requestParameters.setExcludeDiscrete(true);

		DvHydrographReport actual = service.buildReport(requestParameters, "requestingUser", "DV Hydrograph");
		ObjectCompare.compare(buildExpectedWqDvHydrographReport(true, false), actual);

		verify(parameterListService).getParameterMetadata();
		verify(timeSeriesDescriptionService).getTimeSeriesDescriptions(any(DvHydrographRequestParameters.class));
		verify(timeSeriesDataService, times(2)).get(anyString(), any(DvHydrographRequestParameters.class), any(ZoneOffset.class), any(boolean.class), any(boolean.class), any(boolean.class), eq(null));
		verify(locationDescriptionListService).getByLocationIdentifier(anyString());
		verify(dataGapListBuilderService, times(1)).buildGapList(anyList(), any(boolean.class), any(ZoneOffset.class));
		verify(qualifierLookupService).getByQualifierList(anyList());
		verify(fieldVisitDataService, never()).get(anyString());
		verify(fieldVisitDescriptionService, never()).getDescriptions(anyString(), any(ZoneOffset.class), any(DvHydrographRequestParameters.class));

		verify(nwisRaService, never()).getGwLevels(any(DvHydrographRequestParameters.class), anyString(), any(GroundWaterParameter.class), any(ZoneOffset.class));
		verify(nwisRaService, never()).getQwData(any(DvHydrographRequestParameters.class), anyString(), anyString(), any(ZoneOffset.class));
		verify(nwisRaService, never()).getAqParameterNames();
		verify(nwisRaService, never()).getAqParameterUnits();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void buildWqNoPcodeReportTest() {
		ParameterRecord pcodeA = new ParameterRecord();
		pcodeA.setName("nwisName");
		pcodeA.setAlias("unit");
		pcodeA.setCode("pCode");
		ParameterRecord aqname = new ParameterRecord();
		aqname.setAlias("aqname");
		aqname.setName("nwisName");

		given(parameterListService.getParameterMetadata()).willReturn(getParameterMetadata());
		given(timeSeriesDescriptionService.getTimeSeriesDescriptions(any(DvHydrographRequestParameters.class)))
				.willReturn(buildWqTimeSeriesDescriptions("xxx"));
		given(timeSeriesDataService.get(anyString(), any(DvHydrographRequestParameters.class), any(ZoneOffset.class), any(boolean.class), any(boolean.class), any(boolean.class), eq(null)))
				.willReturn(getTimeSeriesDataServiceResponse(true, ZoneOffset.of("-4"), false),
						getTimeSeriesDataServiceResponse(false, ZoneOffset.of("-4"), false));
		given(locationDescriptionListService.getByLocationIdentifier(anyString()))
			.willReturn(new LocationDescription().setIdentifier("0010010000").setName("monitoringLocation"));
		given(dataGapListBuilderService.buildGapList(anyList(), any(boolean.class), any(ZoneOffset.class)))
			.willReturn(getGapList());
		given(qualifierLookupService.getByQualifierList(anyList())).willReturn(metadataMap);
		given(nwisRaService.getQwData(any(DvHydrographRequestParameters.class), anyString(), anyString(), any(ZoneOffset.class)))
			.willReturn(getWaterQualityRecords());
		given(nwisRaService.getAqParameterNames()).willReturn(Arrays.asList(aqname));
		given(nwisRaService.getAqParameterUnits()).willReturn(Arrays.asList(pcodeA));

		DvHydrographRequestParameters requestParameters = new DvHydrographRequestParameters();
		requestParameters.setPrimaryTimeseriesIdentifier("a");
		requestParameters.setSecondStatDerivedIdentifier("c");
		requestParameters.setStartDate(REPORT_START_DATE);
		requestParameters.setEndDate(REPORT_END_DATE);
		requestParameters.setExcludeDiscrete(false);

		DvHydrographReport actual = service.buildReport(requestParameters, "requestingUser", "DV Hydrograph");
		ObjectCompare.compare(buildExpectedWqDvHydrographReport(false, false), actual);

		verify(parameterListService).getParameterMetadata();
		verify(timeSeriesDescriptionService).getTimeSeriesDescriptions(any(DvHydrographRequestParameters.class));
		verify(timeSeriesDataService, times(2)).get(anyString(), any(DvHydrographRequestParameters.class), any(ZoneOffset.class), any(boolean.class), any(boolean.class), any(boolean.class), eq(null));
		verify(locationDescriptionListService).getByLocationIdentifier(anyString());
		verify(dataGapListBuilderService, times(1)).buildGapList(anyList(), any(boolean.class), any(ZoneOffset.class));
		verify(qualifierLookupService).getByQualifierList(anyList());
		verify(fieldVisitDataService, never()).get(anyString());
		verify(fieldVisitDescriptionService, never()).getDescriptions(anyString(), any(ZoneOffset.class), any(DvHydrographRequestParameters.class));

		verify(nwisRaService, never()).getGwLevels(any(DvHydrographRequestParameters.class), anyString(), any(GroundWaterParameter.class), any(ZoneOffset.class));
		verify(nwisRaService, never()).getQwData(any(DvHydrographRequestParameters.class), anyString(), anyString(), any(ZoneOffset.class));
		verify(nwisRaService).getAqParameterNames();
		verify(nwisRaService, never()).getAqParameterUnits();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void buildWqReportTest() {
		ParameterRecord pcodeA = new ParameterRecord();
		pcodeA.setName("nwisName");
		pcodeA.setAlias("unit");
		pcodeA.setCode("pCode");
		ParameterRecord aqname = new ParameterRecord();
		aqname.setAlias("aqname");
		aqname.setName("nwisName");

		given(parameterListService.getParameterMetadata()).willReturn(getParameterMetadata());
		given(timeSeriesDescriptionService.getTimeSeriesDescriptions(any(DvHydrographRequestParameters.class)))
				.willReturn(buildWqTimeSeriesDescriptions("aqname"));
		given(timeSeriesDataService.get(anyString(), any(DvHydrographRequestParameters.class), any(ZoneOffset.class), any(boolean.class), any(boolean.class), any(boolean.class), eq(null)))
				.willReturn(getTimeSeriesDataServiceResponse(true, ZoneOffset.of("-4"), false),
						getTimeSeriesDataServiceResponse(false, ZoneOffset.of("-4"), false));
		given(locationDescriptionListService.getByLocationIdentifier(anyString()))
			.willReturn(new LocationDescription().setIdentifier("0010010000").setName("monitoringLocation"));
		given(dataGapListBuilderService.buildGapList(anyList(), any(boolean.class), any(ZoneOffset.class)))
			.willReturn(getGapList());
		given(qualifierLookupService.getByQualifierList(anyList())).willReturn(metadataMap);
		given(nwisRaService.getQwData(any(DvHydrographRequestParameters.class), anyString(), anyString(), any(ZoneOffset.class)))
			.willReturn(getWaterQualityRecords());
		given(nwisRaService.getAqParameterNames()).willReturn(Arrays.asList(aqname));
		given(nwisRaService.getAqParameterUnits()).willReturn(Arrays.asList(pcodeA));

		DvHydrographRequestParameters requestParameters = new DvHydrographRequestParameters();
		requestParameters.setPrimaryTimeseriesIdentifier("a");
		requestParameters.setSecondStatDerivedIdentifier("c");
		requestParameters.setStartDate(REPORT_START_DATE);
		requestParameters.setEndDate(REPORT_END_DATE);
		requestParameters.setExcludeDiscrete(false);

		DvHydrographReport actual = service.buildReport(requestParameters, "requestingUser", "DV Hydrograph");
		ObjectCompare.compare(buildExpectedWqDvHydrographReport(false, true), actual);

		verify(parameterListService).getParameterMetadata();
		verify(timeSeriesDescriptionService).getTimeSeriesDescriptions(any(DvHydrographRequestParameters.class));
		verify(timeSeriesDataService, times(2)).get(anyString(), any(DvHydrographRequestParameters.class), any(ZoneOffset.class), any(boolean.class), any(boolean.class), any(boolean.class), eq(null));
		verify(locationDescriptionListService).getByLocationIdentifier(anyString());
		verify(dataGapListBuilderService, times(1)).buildGapList(anyList(), any(boolean.class), any(ZoneOffset.class));
		verify(qualifierLookupService).getByQualifierList(anyList());
		verify(fieldVisitDataService, never()).get(anyString());
		verify(fieldVisitDescriptionService, never()).getDescriptions(anyString(), any(ZoneOffset.class), any(DvHydrographRequestParameters.class));

		verify(nwisRaService, never()).getGwLevels(any(DvHydrographRequestParameters.class), anyString(), any(GroundWaterParameter.class), any(ZoneOffset.class));
		verify(nwisRaService).getQwData(any(DvHydrographRequestParameters.class), anyString(), anyString(), any(ZoneOffset.class));
		verify(nwisRaService).getAqParameterNames();
		verify(nwisRaService).getAqParameterUnits();
	}

	@Test
	public void buildTimeSeriesCorrectedDataNullTest() {
		assertNull(service.buildTimeSeriesCorrectedData(null, null, null, null));
		assertNull(service.buildTimeSeriesCorrectedData(new HashMap<String, TimeSeriesDescription>(), null, null, null));
	}

	@Test
	public void buildTimeSeriesCorrectedDataNotFoundAqTest() {
		given(timeSeriesDataService.get(anyString(), any(DvHydrographRequestParameters.class), any(ZoneOffset.class),
				any(boolean.class), any(boolean.class), any(boolean.class), eq(null))).willReturn(null);
		Map<String, TimeSeriesDescription> descriptions = new HashMap<>();
		descriptions.put("abc", new TimeSeriesDescription());
		assertNull(service.buildTimeSeriesCorrectedData(descriptions, "abc", null, null));
		verify(timeSeriesDataService).get(anyString(), any(DvHydrographRequestParameters.class), any(ZoneOffset.class),
		any(boolean.class), any(boolean.class), any(boolean.class), eq(null));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void buildTimeSeriesCorrectedDataTest() {
		boolean endOfPeriod = false;
		ZoneOffset zoneOffset = ZoneOffset.UTC;

		given(timeSeriesDataService.get(anyString(), any(DvHydrographRequestParameters.class), any(ZoneOffset.class),
		any(boolean.class), any(boolean.class), any(boolean.class), eq(null)))
						.willReturn(getTimeSeriesDataServiceResponse(endOfPeriod, zoneOffset, true));
		given(dataGapListBuilderService.buildGapList(anyList(), any(boolean.class), any(ZoneOffset.class)))
				.willReturn(getGapList());

		Map<String, TimeSeriesDescription> descriptions = new HashMap<>();
		descriptions.put("abc", new TimeSeriesDescription());
		TimeSeriesCorrectedData actual = service.buildTimeSeriesCorrectedData(descriptions, "abc", null,
				getParameterMetadata());
		ObjectCompare.compare(getTimeSeriesCorrectedData(endOfPeriod, zoneOffset, true), actual);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void createDvHydroMetadataFirstTest() {
		given(locationDescriptionListService.getByLocationIdentifier(anyString()))
			.willReturn(new LocationDescription().setIdentifier("0010010000").setName("monitoringLocation"));
		given(qualifierLookupService.getByQualifierList(anyList())).willReturn(metadataMap);
		TimeSeriesDataServiceResponse primarySeriesDataResponse = new TimeSeriesDataServiceResponse()
				.setQualifiers(new ArrayList<Qualifier>());
		GroundWaterParameter gwParam = GroundWaterParameter.FWat_LVL_BLSD;
		DvHydrographReportMetadata actual = service.createDvHydroMetadata(buildRequestParameters(),
				buildTimeSeriesDescriptions(), buildPrimarySeriesDescription(), primarySeriesDataResponse, "testUser",
				gwParam, "DV Hydrograph");

		assertThat(actual, samePropertyValuesAs(buildFirstExpectedDvHydroMetadata()));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void createDvHydroMetadataSecondTest() {
		given(locationDescriptionListService.getByLocationIdentifier(anyString()))
			.willReturn(new LocationDescription().setIdentifier("0010010000").setName("monitoringLocation"));
		given(qualifierLookupService.getByQualifierList(anyList())).willReturn(metadataMap);
		TimeSeriesDataServiceResponse primarySeriesDataResponse = new TimeSeriesDataServiceResponse()
				.setQualifiers(new ArrayList<Qualifier>());
		GroundWaterParameter gwParam = GroundWaterParameter.AQ209;
		DvHydrographReportMetadata actual = service.createDvHydroMetadata(buildRequestParameters(),
				buildTimeSeriesDescriptions(), buildPrimarySeriesDescription(), primarySeriesDataResponse, "testUser",
				gwParam, "DV Hydrograph");

		DvHydrographReportMetadata expected = buildFirstExpectedDvHydroMetadata();
		expected.setInverted(false);
		assertThat(actual, samePropertyValuesAs(expected));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void createDvHydroMetadataThirdTest() {
		given(locationDescriptionListService.getByLocationIdentifier(anyString()))
			.willReturn(new LocationDescription().setIdentifier("0010010000").setName("monitoringLocation"));
		given(qualifierLookupService.getByQualifierList(anyList())).willReturn(metadataMap);
		TimeSeriesDataServiceResponse primarySeriesDataResponse = new TimeSeriesDataServiceResponse()
				.setQualifiers(new ArrayList<Qualifier>());
		GroundWaterParameter gwParam = null;
		DvHydrographRequestParameters requestParmeters = buildRequestParameters();
		requestParmeters.setExcludeDiscrete(true);
		requestParmeters.setExcludeMinMax(true);
		requestParmeters.setExcludeZeroNegative(true);
		DvHydrographReportMetadata actual = service.createDvHydroMetadata(requestParmeters,
				buildTimeSeriesDescriptionsII(), buildPrimarySeriesDescription().setUtcOffset(Double.valueOf(4)),
						primarySeriesDataResponse, "testUser",
				gwParam, "DV Hydrograph");

		assertThat(actual, samePropertyValuesAs(buildThirdExpectedDvHydroMetadata()));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void createTimeSeriesCorrectedDataSparseTest() {
		given(dataGapListBuilderService.buildGapList(anyList(), any(boolean.class), any(ZoneOffset.class))).willReturn(null);
		TimeSeriesDataServiceResponse tsd = new TimeSeriesDataServiceResponse();
		TimeSeriesCorrectedData expected = new TimeSeriesCorrectedData();
		expected.setVolumetricFlow(false);
		TimeSeriesCorrectedData actual = service.createTimeSeriesCorrectedData(tsd, true, false, ZoneOffset.UTC);
		ObjectCompare.compare(expected, actual);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void createTimeSeriesCorrectedDataTsUtcTest() {
		given(dataGapListBuilderService.buildGapList(anyList(), any(boolean.class), any(ZoneOffset.class)))
				.willReturn(getGapList());

		boolean endOfPeriod = false;
		ZoneOffset zoneOffset = ZoneOffset.UTC;
		TimeSeriesDataServiceResponse tsd = getTimeSeriesDataServiceResponse(endOfPeriod, zoneOffset, true);
		TimeSeriesCorrectedData expected = getTimeSeriesCorrectedData(endOfPeriod, zoneOffset, true);

		TimeSeriesCorrectedData actual = service.createTimeSeriesCorrectedData(tsd, false, true, ZoneOffset.UTC);
		ObjectCompare.compare(expected, actual);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void createTimeSeriesCorrectedDataDvZ6Test() {
		given(dataGapListBuilderService.buildGapList(anyList(), any(boolean.class), any(ZoneOffset.class)))
		.willReturn(getGapList());

		boolean endOfPeriod = true;
		ZoneOffset zoneOffset = ZoneOffset.of("-6");
		TimeSeriesDataServiceResponse tsd = getTimeSeriesDataServiceResponse(endOfPeriod, zoneOffset, true);
		TimeSeriesCorrectedData expected = getTimeSeriesCorrectedData(endOfPeriod, zoneOffset, true);

		TimeSeriesCorrectedData actual = service.createTimeSeriesCorrectedData(tsd, true, true, ZoneOffset.ofHours(-6));
		ObjectCompare.compare(expected, actual);
	}

	@Test
	public void createDvHydroPointEmptyListTest() {
		assertTrue(service.createDvHydroPoints(new ArrayList<TimeSeriesPoint>(), false, null).isEmpty());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void createDvHydroPointsDvUtcTest() {
		List<DvHydrographPoint> actual = service.createDvHydroPoints(getTimeSeriesPoints(true, ZoneOffset.UTC), true,
				ZoneOffset.UTC);
		assertThat(actual, containsInAnyOrder(samePropertyValuesAs(getDvPoint1(true, ZoneOffset.UTC)),
				samePropertyValuesAs(getDvPoint2(true, ZoneOffset.UTC)),
				samePropertyValuesAs(getDvPoint3(true, ZoneOffset.UTC)),
				samePropertyValuesAs(getDvPoint4(true, ZoneOffset.UTC)),
				samePropertyValuesAs(getDvPoint5(true, ZoneOffset.UTC))));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void createDvHydroPointsTsCtTest() {
		boolean endOfPeriod = true;
		ZoneOffset zoneOffset = ZoneOffset.of("-6");
		List<DvHydrographPoint> actual = service.createDvHydroPoints(getTimeSeriesPoints(endOfPeriod, zoneOffset),
				endOfPeriod, zoneOffset);
		assertEquals(5, actual.size());
		assertThat(actual, containsInAnyOrder(samePropertyValuesAs(getDvPoint1(endOfPeriod, zoneOffset)),
				samePropertyValuesAs(getDvPoint2(endOfPeriod, zoneOffset)),
				samePropertyValuesAs(getDvPoint3(endOfPeriod, zoneOffset)),
				samePropertyValuesAs(getDvPoint4(endOfPeriod, zoneOffset)),
				samePropertyValuesAs(getDvPoint5(endOfPeriod, zoneOffset))));
	}

	@Test
	public void getEstimatedPeriodsEmptyListTest() {
		assertTrue(service.getEstimatedPeriods(new ArrayList<Qualifier>()).isEmpty());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void getEstimatedPeriodsTest() {
		List<InstantRange> actual = service.getEstimatedPeriods(getQualifiers());
		assertEquals(5, actual.size());
		assertThat(actual, containsInAnyOrder(samePropertyValuesAs(getInstantRange1()), samePropertyValuesAs(getInstantRange2()),
				samePropertyValuesAs(getInstantRange3()), samePropertyValuesAs(getInstantRange4()), samePropertyValuesAs(getInstantRange5())));
	}

	@Test
	public void getWaterdataUrlNullTest() {
		assertNull(service.getWaterdataUrl(null));

		assertNull(service.getWaterdataUrl("stationid"));

		ReflectionTestUtils.setField(service, "waterdataUrl", "www.hi.org");
		assertNull(service.getWaterdataUrl(null));
	}

	@Test
	public void getWaterdataUrlTest() {
		ReflectionTestUtils.setField(service, "waterdataUrl", "www.hi.org");
		assertEquals("www.hi.org?site_no=stationid", service.getWaterdataUrl("stationid"));
	}

	@Test
	public void getMinMaxDataEmptyListTest() {
		MinMaxData minMaxData = service.getMinMaxData(new ArrayList<TimeSeriesPoint>());
		assertNotNull(minMaxData);
		assertNotNull(minMaxData.getMin());
		assertTrue(minMaxData.getMin().isEmpty());
		assertNotNull(minMaxData.getMax());
		assertTrue(minMaxData.getMax().isEmpty());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void getMinMaxDataDvTest() {
		boolean endOfPeriod = true;
		ZoneOffset zoneOffset = ZoneOffset.of("-6");
		MinMaxData minMaxData = service.getMinMaxData(getTimeSeriesPoints(endOfPeriod, zoneOffset));
		assertNotNull(minMaxData);
		assertNotNull(minMaxData.getMin());
		assertEquals(3, minMaxData.getMin().size());
		assertThat(minMaxData.getMin(),
				contains(samePropertyValuesAs(getMinMaxPoint5(endOfPeriod, zoneOffset)),
						samePropertyValuesAs(getMinMaxPoint4(endOfPeriod, zoneOffset)),
						samePropertyValuesAs(getMinMaxPoint2(endOfPeriod, zoneOffset))));
		assertNotNull(minMaxData.getMax());
		assertEquals(1, minMaxData.getMax().size());
		assertThat(minMaxData.getMax(), contains(samePropertyValuesAs(getMinMaxPoint3(endOfPeriod, zoneOffset))));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void getMinMaxDataTsTest() {
		boolean endOfPeriod = false;
		ZoneOffset zoneOffset = ZoneOffset.UTC;
		MinMaxData minMaxData = service.getMinMaxData(getTimeSeriesPoints(endOfPeriod, zoneOffset));
		assertNotNull(minMaxData);
		assertNotNull(minMaxData.getMin());
		assertEquals(3, minMaxData.getMin().size());
		assertThat(minMaxData.getMin(),
				contains(samePropertyValuesAs(getMinMaxPoint5(endOfPeriod, zoneOffset)),
						samePropertyValuesAs(getMinMaxPoint4(endOfPeriod, zoneOffset)),
						samePropertyValuesAs(getMinMaxPoint2(endOfPeriod, zoneOffset))));
		assertNotNull(minMaxData.getMax());
		assertEquals(1, minMaxData.getMax().size());
		assertThat(minMaxData.getMax(), contains(samePropertyValuesAs(getMinMaxPoint3(endOfPeriod, zoneOffset))));
	}

	// --------------------------------------------------------------------------------------------------

	protected DvHydrographRequestParameters buildRequestParameters() {
		DvHydrographRequestParameters requestParameters = new DvHydrographRequestParameters();
		requestParameters.setPrimaryTimeseriesIdentifier("a");
		requestParameters.setFirstStatDerivedIdentifier("b");
		requestParameters.setSecondStatDerivedIdentifier("c");
		requestParameters.setThirdStatDerivedIdentifier("d");
		requestParameters.setFourthStatDerivedIdentifier("e");
		requestParameters.setFirstReferenceIdentifier("f");
		requestParameters.setSecondReferenceIdentifier("g");
		requestParameters.setThirdReferenceIdentifier("h");
		requestParameters.setComparisonTimeseriesIdentifier("i");
		requestParameters.setStartDate(REPORT_START_DATE);
		requestParameters.setEndDate(REPORT_END_DATE);
		return requestParameters;
	}

	protected Map<String, TimeSeriesDescription> buildTimeSeriesDescriptions() {
		Map<String, TimeSeriesDescription> descriptions = new HashMap<>();
		descriptions.put("a", buildPrimarySeriesDescription());
		descriptions.put("b", buildPrimarySeriesDescription().setIdentifier("firstStatDerived"));
		descriptions.put("c", buildPrimarySeriesDescription().setIdentifier("secondStatDerived"));
		descriptions.put("d", buildPrimarySeriesDescription().setIdentifier("thirdStatDerived"));
		descriptions.put("e", buildPrimarySeriesDescription().setIdentifier("fourthStatDerived"));
		descriptions.put("f", buildPrimarySeriesDescription().setIdentifier("firstRefTS"));
		descriptions.put("g", buildPrimarySeriesDescription().setIdentifier("secondRefTS"));
		descriptions.put("h", buildPrimarySeriesDescription().setIdentifier("thirdRefTS"));
		descriptions.put("i", buildPrimarySeriesDescription().setIdentifier("comparison"));
		return descriptions;
	};

	protected Map<String, TimeSeriesDescription> buildGwTimeSeriesDescriptions() {
		Map<String, TimeSeriesDescription> descriptions = new HashMap<>();
		descriptions.put("a", buildPrimarySeriesDescription().setParameter("WaterLevel, BelowLSD"));
		descriptions.put("b", buildPrimarySeriesDescription().setIdentifier("firstStatDerived"));
		return descriptions;
	};

	protected Map<String, TimeSeriesDescription> buildWqTimeSeriesDescriptions(String parameter) {
		Map<String, TimeSeriesDescription> descriptions = new HashMap<>();
		descriptions.put("a", buildPrimarySeriesDescription().setParameter(parameter).setUnit("unit"));
		descriptions.put("c", buildPrimarySeriesDescription().setIdentifier("secondStatDerived"));
		return descriptions;
	};

	protected DvHydrographReport buildExpectedDvHydrographReport() {
		DvHydrographReportMetadata reportMetadata = buildFirstExpectedDvHydroMetadata();
		reportMetadata.setInverted(false);
		DvHydrographReport expected = new DvHydrographReport();
		expected.setComparisonSeries(getTimeSeriesCorrectedData(false, ZoneOffset.ofHours(-4), true));
		expected.setFieldVisitMeasurements(getFieldVisitMeasurements());
		expected.setFirstStatDerived(getTimeSeriesCorrectedData(false, ZoneOffset.ofHours(-4), true));
		expected.setFirstReferenceTimeSeries(getTimeSeriesCorrectedData(false, ZoneOffset.ofHours(-4), true));
		expected.setFourthStatDerived(getTimeSeriesCorrectedData(false, ZoneOffset.ofHours(-4), true));
		expected.setMaxMinData(getMaxMinData(true, ZoneOffset.ofHours(-4)));
		expected.setPrimarySeriesApprovals(getApprovals());
		expected.setPrimarySeriesQualifiers(getQualifiers());
		expected.setReportMetadata(reportMetadata);
		expected.setSecondReferenceTimeSeries(getTimeSeriesCorrectedData(false, ZoneOffset.ofHours(-4), true));
		expected.setSecondStatDerived(getTimeSeriesCorrectedData(false, ZoneOffset.ofHours(-4), true));
		expected.setThirdReferenceTimeSeries(getTimeSeriesCorrectedData(false, ZoneOffset.ofHours(-4), true));
		expected.setThirdStatDerived(getTimeSeriesCorrectedData(false, ZoneOffset.ofHours(-4), true));
		return expected;
	}

	protected DvHydrographReport buildExpectedGwDvHydrographReport(boolean isExcludeDiscrete) {
		DvHydrographReportMetadata reportMetadata = buildExpectedGwDvHydroMetadata();
		reportMetadata.setInverted(true);
		reportMetadata.setExcludeDiscrete(isExcludeDiscrete);
		DvHydrographReport expected = new DvHydrographReport();
		expected.setFirstStatDerived(getTimeSeriesCorrectedData(false, ZoneOffset.ofHours(-4), true));
		expected.setMaxMinData(getMaxMinData(true, ZoneOffset.ofHours(-4)));
		expected.setPrimarySeriesApprovals(getApprovals());
		expected.setPrimarySeriesQualifiers(getQualifiers());
		expected.setReportMetadata(reportMetadata);
		if (!isExcludeDiscrete) {
			expected.setGwlevel(getGwLevels().getRecords());
		}
		return expected;
	}

	protected DvHydrographReport buildExpectedWqDvHydrographReport(boolean isExcludeDiscrete, boolean expectData) {
		DvHydrographReportMetadata reportMetadata = buildExpectedWqDvHydroMetadata();
		reportMetadata.setInverted(false);
		reportMetadata.setExcludeDiscrete(isExcludeDiscrete);
		DvHydrographReport expected = new DvHydrographReport();
		expected.setSecondStatDerived(getTimeSeriesCorrectedData(false, ZoneOffset.ofHours(-4), false));
		expected.setPrimarySeriesApprovals(getApprovals());
		expected.setPrimarySeriesQualifiers(getQualifiers());
		expected.setReportMetadata(reportMetadata);
		if (!isExcludeDiscrete && expectData) {
			expected.setWaterQuality(getWaterQualityRecords());
		}
		return expected;
	}

	protected TimeSeriesDescription buildPrimarySeriesDescription() {
		return new TimeSeriesDescription()
				.setIdentifier("primaryIdentifier")
				.setUtcOffset(Double.valueOf(-4))
				.setParameter("Discharge");
	}
	protected Map<String, TimeSeriesDescription> buildTimeSeriesDescriptionsII() {
		Map<String, TimeSeriesDescription> descriptions = new HashMap<>();
		descriptions.put("a", buildPrimarySeriesDescription().setUtcOffset(Double.valueOf(4)));
		return descriptions;
	};

	protected DvHydrographReportMetadata buildFirstExpectedDvHydroMetadata() {
		DvHydrographReportMetadata expected = new DvHydrographReportMetadata();
		expected.setTimezone("Etc/GMT+4");
		expected.setStartDate(REPORT_START_INSTANT);
		expected.setEndDate(REPORT_END_INSTANT);
		expected.setTitle("DV Hydrograph");
		expected.setStationName("monitoringLocation");
		expected.setStationId("0010010000");
		expected.setQualifierMetadata(metadataMap);

		expected.setFirstStatDerivedLabel("firstStatDerived");
		expected.setInverted(true);
		expected.setPrimarySeriesLabel("primaryIdentifier");
		expected.setComparisonSeriesLabel("comparison");
		expected.setExcludeDiscrete(false);
		expected.setExcludeZeroNegative(false);
		expected.setExcludeMinMax(false);
		expected.setFirstReferenceTimeSeriesLabel("firstRefTS");
		expected.setFourthStatDerivedLabel("fourthStatDerived");
		expected.setSecondReferenceTimeSeriesLabel("secondRefTS");
		expected.setSecondStatDerivedLabel("secondStatDerived");
		expected.setThirdReferenceTimeSeriesLabel("thirdRefTS");
		expected.setThirdStatDerivedLabel("thirdStatDerived");
		return expected;
	}

	protected DvHydrographReportMetadata buildThirdExpectedDvHydroMetadata() {
		DvHydrographReportMetadata expected = new DvHydrographReportMetadata();
		expected.setTimezone("Etc/GMT-4");
		expected.setStartDate(REPORT_START_INSTANT);
		expected.setEndDate(REPORT_END_INSTANT);
		expected.setTitle("DV Hydrograph");
		expected.setStationName("monitoringLocation");
		expected.setStationId("0010010000");
		expected.setQualifierMetadata(metadataMap);

		expected.setFirstStatDerivedLabel(null);
		expected.setInverted(false);
		expected.setPrimarySeriesLabel("primaryIdentifier");
		expected.setComparisonSeriesLabel(null);
		expected.setExcludeDiscrete(true);
		expected.setExcludeZeroNegative(true);
		expected.setExcludeMinMax(true);
		expected.setFirstReferenceTimeSeriesLabel(null);
		expected.setFourthStatDerivedLabel(null);
		expected.setSecondReferenceTimeSeriesLabel(null);
		expected.setSecondStatDerivedLabel(null);
		expected.setThirdReferenceTimeSeriesLabel(null);
		expected.setThirdStatDerivedLabel(null);
		return expected;
	}

	protected DvHydrographReportMetadata buildExpectedGwDvHydroMetadata() {
		DvHydrographReportMetadata expected = new DvHydrographReportMetadata();
		expected.setTimezone("Etc/GMT+4");
		expected.setStartDate(REPORT_START_INSTANT);
		expected.setEndDate(REPORT_END_INSTANT);
		expected.setTitle("DV Hydrograph");
		expected.setStationName("monitoringLocation");
		expected.setStationId("0010010000");
		expected.setQualifierMetadata(metadataMap);

		expected.setFirstStatDerivedLabel("firstStatDerived");
		expected.setInverted(true);
		expected.setPrimarySeriesLabel("primaryIdentifier");
		expected.setExcludeDiscrete(false);
		expected.setExcludeZeroNegative(false);
		expected.setExcludeMinMax(false);
		return expected;
	}

	protected DvHydrographReportMetadata buildExpectedWqDvHydroMetadata() {
		DvHydrographReportMetadata expected = new DvHydrographReportMetadata();
		expected.setTimezone("Etc/GMT+4");
		expected.setStartDate(REPORT_START_INSTANT);
		expected.setEndDate(REPORT_END_INSTANT);
		expected.setTitle("DV Hydrograph");
		expected.setStationName("monitoringLocation");
		expected.setStationId("0010010000");
		expected.setQualifierMetadata(metadataMap);

		expected.setSecondStatDerivedLabel("secondStatDerived");
		expected.setInverted(true);
		expected.setPrimarySeriesLabel("primaryIdentifier");
		expected.setExcludeDiscrete(false);
		expected.setExcludeZeroNegative(false);
		expected.setExcludeMinMax(false);
		return expected;
	}

	protected Map<String, QualifierMetadata> buildQualifierMetadata() {
		Map<String, QualifierMetadata> metadata = new HashMap<>();
		metadata.put("a", QUALIFIER_METADATA_A);
		metadata.put("b", QUALIFIER_METADATA_B);
		metadata.put("c", QUALIFIER_METADATA_C);
		metadata.put("d", QUALIFIER_METADATA_D);
		return metadata;
	}

	protected ArrayList<TimeSeriesPoint> getTimeSeriesPoints(boolean endOfPeriod, ZoneOffset zoneOffset) {
		ArrayList<TimeSeriesPoint> timeSeriesPoints = Stream
				.of(getTsPoint1(endOfPeriod, zoneOffset),
					getTsPoint2(endOfPeriod, zoneOffset),
					getTsPoint3(endOfPeriod, zoneOffset),
					getTsPoint4(endOfPeriod, zoneOffset),
					getTsPoint5(endOfPeriod, zoneOffset),
					getTsPoint6(endOfPeriod, zoneOffset))
				.collect(Collectors.toCollection(ArrayList::new));
		return timeSeriesPoints;
	}

	protected TimeSeriesPoint getTsPoint1(boolean endOfPeriod, ZoneOffset zoneOffset) {
		return new TimeSeriesPoint()
				.setValue(new DoubleWithDisplay().setDisplay("654.321").setNumeric(Double.valueOf("123.456")))
				.setTimestamp(new StatisticalDateTimeOffset().setDateTimeOffset(getTestInstant(endOfPeriod, zoneOffset, 6))
						.setRepresentsEndOfTimePeriod(endOfPeriod));
	}
	protected TimeSeriesPoint getTsPoint2(boolean endOfPeriod, ZoneOffset zoneOffset) {
		return new TimeSeriesPoint()
				.setValue(new DoubleWithDisplay().setDisplay("321.987").setNumeric(Double.valueOf("789.123")))
				.setTimestamp(new StatisticalDateTimeOffset().setDateTimeOffset(getTestInstant(endOfPeriod, zoneOffset, 2))
						.setRepresentsEndOfTimePeriod(endOfPeriod));
	}
	protected TimeSeriesPoint getTsPoint3(boolean endOfPeriod, ZoneOffset zoneOffset) {
		return new TimeSeriesPoint()
				.setValue(new DoubleWithDisplay().setDisplay("987.654").setNumeric(Double.valueOf("456.789")))
				.setTimestamp(new StatisticalDateTimeOffset().setDateTimeOffset(getTestInstant(endOfPeriod, zoneOffset, 0))
						.setRepresentsEndOfTimePeriod(endOfPeriod));
	}
	protected TimeSeriesPoint getTsPoint4(boolean endOfPeriod, ZoneOffset zoneOffset) {
		return new TimeSeriesPoint()
				.setValue(new DoubleWithDisplay().setDisplay("321.987").setNumeric(Double.valueOf("789.123")))
				.setTimestamp(new StatisticalDateTimeOffset().setDateTimeOffset(getTestInstant(endOfPeriod, zoneOffset, 4))
						.setRepresentsEndOfTimePeriod(endOfPeriod));
	}
	protected TimeSeriesPoint getTsPoint5(boolean endOfPeriod, ZoneOffset zoneOffset) {
		return new TimeSeriesPoint()
				.setValue(new DoubleWithDisplay().setDisplay("321.987").setNumeric(Double.valueOf("789.123")))
				.setTimestamp(new StatisticalDateTimeOffset().setDateTimeOffset(getTestInstant(endOfPeriod, zoneOffset, 5))
						.setRepresentsEndOfTimePeriod(endOfPeriod));
	}
	protected TimeSeriesPoint getTsPoint6(boolean endOfPeriod, ZoneOffset zoneOffset) {
		return new TimeSeriesPoint()
				.setValue(new DoubleWithDisplay().setDisplay("EMPTY").setNumeric(null))
				.setTimestamp(new StatisticalDateTimeOffset().setDateTimeOffset(getTestInstant(endOfPeriod, zoneOffset, 12))
						.setRepresentsEndOfTimePeriod(endOfPeriod));
	}
	protected Instant getTestInstant(boolean endOfPeriod, ZoneOffset zoneOffset, long days) {
		if (endOfPeriod) {
			//In the world of Aquarius, Daily Values are at 24:00 of the day of measurement, which is actually
			//00:00 of the next day in (most) all other realities.
			//For testing, this means we need to back up one day from what would be expected.
			return nowLocalDate.atTime(0, 0, 0).toInstant(zoneOffset).minus(Duration.ofDays(days-1));
		} else {
			return nowInstant.minus(Duration.ofDays(days));
		}
	}

	protected MinMaxPoint getMinMaxPoint1(boolean endOfPeriod, ZoneOffset zoneOffset) {
		return new MinMaxPoint(getTestInstant(endOfPeriod, zoneOffset, 6), new BigDecimal("654.321"));
	}
	protected MinMaxPoint getMinMaxPoint2(boolean endOfPeriod, ZoneOffset zoneOffset) {
		return new MinMaxPoint(getTestInstant(endOfPeriod, zoneOffset, 2), new BigDecimal("321.987"));
	}
	protected MinMaxPoint getMinMaxPoint3(boolean endOfPeriod, ZoneOffset zoneOffset) {
		return new MinMaxPoint(getTestInstant(endOfPeriod, zoneOffset, 0), new BigDecimal("987.654"));
	}
	protected MinMaxPoint getMinMaxPoint4(boolean endOfPeriod, ZoneOffset zoneOffset) {
		return new MinMaxPoint(getTestInstant(endOfPeriod, zoneOffset, 4), new BigDecimal("321.987"));
	}
	protected MinMaxPoint getMinMaxPoint5(boolean endOfPeriod, ZoneOffset zoneOffset) {
		return new MinMaxPoint(getTestInstant(endOfPeriod, zoneOffset, 5), new BigDecimal("321.987"));
	}

	protected Map<String, ParameterMetadata> getParameterMetadata() {
		Map<String, ParameterMetadata> parameterMetadata = new HashMap<>();
		parameterMetadata.put("abc", new ParameterMetadata().setIdentifier("abc")
				.setUnitGroupIdentifier("dalek"));
		parameterMetadata.put("def", new ParameterMetadata().setIdentifier("def")
				.setUnitGroupIdentifier(ReportBuilderService.VOLUMETRIC_FLOW_UNIT_GROUP_VALUE));
		return parameterMetadata;
	}

	protected DvHydrographPoint getDvPoint1(boolean isDv, ZoneOffset zoneOffset) {
		DvHydrographPoint dvPoint = new DvHydrographPoint();
		Instant time = nowInstant.minus(Duration.ofDays(6));
		if (isDv) {
			dvPoint.setTime(LocalDate.from(time.atOffset(zoneOffset)));
		} else {
			dvPoint.setTime(time);
		}
		dvPoint.setValue(new BigDecimal("654.321"));
		return dvPoint;
	}
	protected DvHydrographPoint getDvPoint2(boolean isDv, ZoneOffset zoneOffset) {
		DvHydrographPoint dvPoint = new DvHydrographPoint();
		Instant time = nowInstant.minus(Duration.ofDays(2));
		if (isDv) {
			dvPoint.setTime(LocalDate.from(time.atOffset(zoneOffset)));
		} else {
		dvPoint.setTime(time);
		}
		dvPoint.setValue(new BigDecimal("321.987"));
		return dvPoint;
	}
	protected DvHydrographPoint getDvPoint3(boolean isDv, ZoneOffset zoneOffset) {
		DvHydrographPoint dvPoint = new DvHydrographPoint();
		Instant time = nowInstant;
		if (isDv) {
			dvPoint.setTime(LocalDate.from(time.atOffset(zoneOffset)));
		} else {
		dvPoint.setTime(time);
		}
		dvPoint.setValue(new BigDecimal("987.654"));
		return dvPoint;
	}
	protected DvHydrographPoint getDvPoint4(boolean isDv, ZoneOffset zoneOffset) {
		DvHydrographPoint dvPoint = new DvHydrographPoint();
		Instant time = nowInstant.minus(Duration.ofDays(4));
		if (isDv) {
			dvPoint.setTime(LocalDate.from(time.atOffset(zoneOffset)));
		} else {
		dvPoint.setTime(time);
		}
		dvPoint.setValue(new BigDecimal("321.987"));
		return dvPoint;
	}
	protected DvHydrographPoint getDvPoint5(boolean isDv, ZoneOffset zoneOffset) {
		DvHydrographPoint dvPoint = new DvHydrographPoint();
		Instant time = nowInstant.minus(Duration.ofDays(5));
		if (isDv) {
			dvPoint.setTime(LocalDate.from(time.atOffset(zoneOffset)));
		} else {
			dvPoint.setTime(time);
		}
		dvPoint.setValue(new BigDecimal("321.987"));
		return dvPoint;
	}

	protected List<Qualifier> getQualifiers() {
		return Stream.of(
				buildQualifier("skipMe1", nowInstant.minus(Duration.ofDays(2)), nowInstant.minus(Duration.ofDays(1))),
				buildQualifier(ReportBuilderService.ESTIMATED_QUALIFIER_VALUE, nowInstant.minus(Duration.ofDays(4)),
						nowInstant.minus(Duration.ofDays(3))),
				buildQualifier("skipMe2", nowInstant.minus(Duration.ofDays(6)), nowInstant.minus(Duration.ofDays(5))),
				buildQualifier(ReportBuilderService.ESTIMATED_QUALIFIER_VALUE, nowInstant.minus(Duration.ofDays(8)),
						nowInstant.minus(Duration.ofDays(7))),
				buildQualifier(ReportBuilderService.ESTIMATED_QUALIFIER_VALUE, nowInstant.minus(Duration.ofDays(10)),
						nowInstant.minus(Duration.ofDays(9))),
				buildQualifier(ReportBuilderService.ESTIMATED_QUALIFIER_VALUE, nowInstant.minus(Duration.ofDays(12)),
						nowInstant.minus(Duration.ofDays(11))),
				buildQualifier("skipMe3", nowInstant.minus(Duration.ofDays(14)), nowInstant.minus(Duration.ofDays(13))),
				buildQualifier("skipMe4", nowInstant.minus(Duration.ofDays(16)), nowInstant.minus(Duration.ofDays(15))),
				buildQualifier(ReportBuilderService.ESTIMATED_QUALIFIER_VALUE, nowInstant.minus(Duration.ofDays(18)),
						nowInstant.minus(Duration.ofDays(17))),
				buildQualifier("skipMe5", nowInstant.minus(Duration.ofDays(20)), nowInstant.minus(Duration.ofDays(19))))
				.collect(Collectors.toList());
	}

	protected Qualifier buildQualifier(String identifier, Instant start, Instant end) {
		Qualifier qualifier = new Qualifier().setIdentifier(identifier);
		qualifier.setStartTime(start).setEndTime(end);
		return qualifier;
	}

	protected InstantRange getInstantRange1() {
		return new InstantRange(nowInstant.minus(Duration.ofDays(4)), nowInstant.minus(Duration.ofDays(3)));
	}
	protected InstantRange getInstantRange2() {
		return new InstantRange(nowInstant.minus(Duration.ofDays(8)), nowInstant.minus(Duration.ofDays(7)));
	}
	protected InstantRange getInstantRange3() {
		return new InstantRange(nowInstant.minus(Duration.ofDays(10)), nowInstant.minus(Duration.ofDays(9)));
	}
	protected InstantRange getInstantRange4() {
		return new InstantRange(nowInstant.minus(Duration.ofDays(12)), nowInstant.minus(Duration.ofDays(11)));
	}
	protected InstantRange getInstantRange5() {
		return new InstantRange(nowInstant.minus(Duration.ofDays(18)), nowInstant.minus(Duration.ofDays(17)));
	}

	protected List<DataGap> getGapList() {
		return Stream
				.of(new DataGap(getTestInstant(false, ZoneOffset.UTC, 10), getTestInstant(false, ZoneOffset.UTC, 8)),
					new DataGap(getTestInstant(false, ZoneOffset.UTC, 5), getTestInstant(false, ZoneOffset.UTC, 3)))
				.collect(Collectors.toList());
	}

	protected List<Approval> getApprovals() {
		return Stream.of(getApproval1(), getApproval2()).collect(Collectors.toList());
	}
	protected Approval getApproval1() {
		Approval approval = new Approval().setApprovalLevel(1200).setDateAppliedUtc(nowInstant).setUser("admin")
				.setLevelDescription("Approved").setComment("wow");
		approval.setStartTime(Instant.parse("2000-01-01T00:00:00.0000000Z"));
		approval.setEndTime(InstantDeserializer.MaxConcreteValue);
		return approval;
	}
	protected Approval getApproval2() {
		Approval approval = new Approval().setApprovalLevel(900).setDateAppliedUtc(nowInstant).setUser("admin")
				.setLevelDescription("Working").setComment("wow");
		approval.setStartTime(InstantDeserializer.MinConcreteValue);
		approval.setEndTime(Instant.parse("1999-12-31T23:59:59.9999999Z"));
		return approval;
	}

	protected List<GapTolerance> getGapTolerances() {
		return Stream.of(new GapTolerance(), new GapTolerance()).collect(Collectors.toList());
	}

	protected TimeSeriesDataServiceResponse getTimeSeriesDataServiceResponse(boolean endOfPeriod, ZoneOffset zoneOffset, boolean withPoints) {
		ArrayList<Qualifier> qualifiers = new ArrayList<>(getQualifiers());

		TimeSeriesDataServiceResponse timeSeriesDataServiceResponse = new TimeSeriesDataServiceResponse()
				.setTimeRange(new StatisticalTimeRange()
						.setStartTime(new StatisticalDateTimeOffset()
								.setDateTimeOffset(getTestInstant(endOfPeriod, zoneOffset, 2))
								.setRepresentsEndOfTimePeriod(endOfPeriod))
						.setEndTime(new StatisticalDateTimeOffset()
								.setDateTimeOffset(getTestInstant(endOfPeriod, zoneOffset, 0))
								.setRepresentsEndOfTimePeriod(endOfPeriod)))
				.setUnit("myUnit")
				.setParameter("def")
				.setQualifiers(qualifiers)
				.setApprovals(new ArrayList<Approval>(getApprovals()))
				.setGapTolerances(new ArrayList<GapTolerance>(getGapTolerances()));

		if (withPoints) {
			timeSeriesDataServiceResponse.setPoints(getTimeSeriesPoints(endOfPeriod, zoneOffset));
		}

		return timeSeriesDataServiceResponse;
	}

	protected TimeSeriesCorrectedData getTimeSeriesCorrectedData(boolean endOfPeriod, ZoneOffset zoneOffset, boolean withPoints) {
		TimeSeriesCorrectedData timeSeriesCorrectedData = new TimeSeriesCorrectedData();
		if (withPoints) {
			timeSeriesCorrectedData.setPoints(Arrays.asList(getDvPoint1(endOfPeriod, zoneOffset),
					getDvPoint2(endOfPeriod, zoneOffset), getDvPoint3(endOfPeriod, zoneOffset),
					getDvPoint4(endOfPeriod, zoneOffset), getDvPoint5(endOfPeriod, zoneOffset)));
		}
		timeSeriesCorrectedData.setType("def");
		timeSeriesCorrectedData.setUnit("myUnit");
		if (endOfPeriod) {
			//Testing a DV
			timeSeriesCorrectedData.setEndTime(nowLocalDate);
			timeSeriesCorrectedData.setStartTime(nowLocalDate.minusDays(2));
		} else {
			//Testing instantaneous
			timeSeriesCorrectedData.setEndTime(nowInstant);
			timeSeriesCorrectedData.setStartTime(nowInstant.minus(Duration.ofDays(2)));
		}
		timeSeriesCorrectedData.setVolumetricFlow(true);
		timeSeriesCorrectedData.setEstimatedPeriods(Arrays.asList(getInstantRange1(), getInstantRange2(),
				getInstantRange3(), getInstantRange4(), getInstantRange5()));
		timeSeriesCorrectedData.setApprovals(getApprovals());
		timeSeriesCorrectedData.setGaps(getGapList());
		timeSeriesCorrectedData.setGapTolerances(getGapTolerances());
		return timeSeriesCorrectedData;
	}

	protected MinMaxData getMaxMinData(boolean endOfPeriod, ZoneOffset zoneOffset) {
		Map<BigDecimal, List<MinMaxPoint>> points = Stream
				.of(getMinMaxPoint5(endOfPeriod, zoneOffset), getMinMaxPoint4(endOfPeriod, zoneOffset),
						getMinMaxPoint2(endOfPeriod, zoneOffset), getMinMaxPoint3(endOfPeriod, zoneOffset))
				.collect(Collectors.groupingByConcurrent(MinMaxPoint::getValue));
		return new MinMaxData(BigDecimal.valueOf(321.987), BigDecimal.valueOf(987.654), points);
	}

	protected List<FieldVisitDescription> getFieldVisitDecriptions() {
		return Stream
				.of(new FieldVisitDescription().setIdentifier("a"), new FieldVisitDescription().setIdentifier("b"))
				.collect(Collectors.toList());
	}

	protected FieldVisitDataServiceResponse getFieldVisitDataServiceResponse(ArrayList<DischargeActivity> activities) {
		return new FieldVisitDataServiceResponse().setDischargeActivities(activities);
	}

	protected ArrayList<DischargeActivity> getActivities() {
		ArrayList<DischargeActivity> activities = Stream
			.of(new DischargeActivity()
					.setDischargeSummary(
							new DischargeSummary().setMeasurementGrade(MeasurementGradeType.Good)
									.setMeanGageHeight((QuantityWithDisplay) new QuantityWithDisplay()
											.setUnit("meanGageHeightUnits").setDisplay("2.0090")
											.setNumeric(Double.valueOf("2.0090")))
									.setDischarge((QuantityWithDisplay) new QuantityWithDisplay()
											.setUnit("dischargeUnits").setDisplay("20.0090")
											.setNumeric(Double.valueOf("20.0090")))
									.setPublish(false)),
				new DischargeActivity()
					.setDischargeSummary(
							new DischargeSummary().setMeasurementGrade(MeasurementGradeType.Excellent)
									.setMeanGageHeight((QuantityWithDisplay) new QuantityWithDisplay()
											.setUnit("meanGageHeightUnits").setDisplay("2.0090")
											.setNumeric(Double.valueOf("2.0090")))
									.setDischarge((QuantityWithDisplay) new QuantityWithDisplay()
											.setUnit("dischargeUnits").setDisplay("20.0090")
											.setNumeric(Double.valueOf("20.0090")))
									.setPublish(false)))
			.collect(Collectors.toCollection(ArrayList::new));
		return activities;
	}

	protected List<FieldVisitMeasurement> getFieldVisitMeasurements() {
		FieldVisitMeasurement f1 = new FieldVisitMeasurement();
		f1.setMeasurementNumber(null);
		f1.setDischarge(BigDecimal.valueOf(20.009));
		f1.setErrorMaxDischarge(BigDecimal.valueOf(21.00945));
		f1.setErrorMinDischarge(BigDecimal.valueOf(19.00855));
		f1.setMeasurementStartDate(null);
		f1.setPublish(false);
		FieldVisitMeasurement f2 = new FieldVisitMeasurement();
		f2.setMeasurementNumber(null);
		f2.setDischarge(BigDecimal.valueOf(20.009));
		f2.setErrorMaxDischarge(BigDecimal.valueOf(20.40918));
		f2.setErrorMinDischarge(BigDecimal.valueOf(19.60882));
		f2.setMeasurementStartDate(null);
		f2.setPublish(false);
		FieldVisitMeasurement f3 = new FieldVisitMeasurement();
		f3.setMeasurementNumber(null);
		f3.setDischarge(BigDecimal.valueOf(20.009));
		f3.setErrorMaxDischarge(BigDecimal.valueOf(21.00945));
		f3.setErrorMinDischarge(BigDecimal.valueOf(19.00855));
		f3.setMeasurementStartDate(null);
		f3.setPublish(false);
		FieldVisitMeasurement f4 = new FieldVisitMeasurement();
		f4.setMeasurementNumber(null);
		f4.setDischarge(BigDecimal.valueOf(20.009));
		f4.setErrorMaxDischarge(BigDecimal.valueOf(20.40918));
		f4.setErrorMinDischarge(BigDecimal.valueOf(19.60882));
		f4.setMeasurementStartDate(null);
		f4.setPublish(false);


		return Arrays.asList(f1,f2);
	}

	protected WaterLevelRecords getGwLevels() {
		WaterLevelRecords toRet = new WaterLevelRecords();
		toRet.setRecords(getGwLevelRecords());
		toRet.setAllValid(true);
		return toRet;
	}

	protected List<WaterLevelRecord> getGwLevelRecords() {
		return Stream.of(getWaterLevelRecord(123.456, nowInstant),
				getWaterLevelRecord(122.334, nowInstant.minusSeconds(3600)))
			.collect(Collectors.toList());
	}
	protected WaterLevelRecord getWaterLevelRecord(Double groundWaterLevel, Instant date) {
		WaterLevelRecord waterLevelRecord = new WaterLevelRecord();
		waterLevelRecord.setDate(date.atOffset(ZoneOffset.of("-6")));
		waterLevelRecord.setGroundWaterLevel(BigDecimal.valueOf(groundWaterLevel));
		waterLevelRecord.setSiteNumber("01010001");
		waterLevelRecord.setTimeZone("CST");
		return waterLevelRecord;
	}

	protected List<WaterQualitySampleRecord> getWaterQualityRecords() {
		return Stream.of(getWaterQualityRecord(123.456, nowInstant),
				getWaterQualityRecord(122.334, nowInstant.minusSeconds(3600)))
			.collect(Collectors.toList());
	}
	protected WaterQualitySampleRecord getWaterQualityRecord(Double val, Instant date) {
		WaterQualitySampleRecord waterQualitySampleRecord = new WaterQualitySampleRecord();
		waterQualitySampleRecord.setSampleStartDateTime(date.atOffset(ZoneOffset.of("-6")));
		WqValue wqValue = new WqValue();
		wqValue.setValue(BigDecimal.valueOf(val));
		waterQualitySampleRecord.setValue(wqValue);
		waterQualitySampleRecord.setTimeZone("CST");
		return waterQualitySampleRecord;
	}
}