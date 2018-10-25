package gov.usgs.aqcu.model;

import java.util.List;

import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.Approval;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.Qualifier;

import gov.usgs.aqcu.model.nwis.WaterLevelRecord;
import gov.usgs.aqcu.model.nwis.WaterQualitySampleRecord;

public class DvHydrographReport {	

	//Required Properties
	private TimeSeriesCorrectedData firstStatDerived;
	private MinMaxData maxMinData;
	private DvHydrographReportMetadata reportMetadata;

	//Optional Properties
	private TimeSeriesCorrectedData comparisonSeries;
	private List<FieldVisitMeasurement> fieldVisitMeasurements;
	private TimeSeriesCorrectedData firstReferenceTimeSeries;
	private TimeSeriesCorrectedData fourthStatDerived;
	private List<WaterLevelRecord> gwlevel;
	private List<Qualifier> primarySeriesQualifiers;
	private List<Approval> primarySeriesApprovals;
	private TimeSeriesCorrectedData secondReferenceTimeSeries;
	private TimeSeriesCorrectedData secondStatDerived;
	private String simsUrl;
	private TimeSeriesCorrectedData thirdReferenceTimeSeries;
	private TimeSeriesCorrectedData thirdStatDerived;
	private String waterdataUrl;
	private List<WaterQualitySampleRecord> waterQuality;

	public TimeSeriesCorrectedData getFirstStatDerived() {
		return firstStatDerived;
	}
	public void setFirstStatDerived(TimeSeriesCorrectedData firstStatDerived) {
		this.firstStatDerived = firstStatDerived;
	}
	public MinMaxData getMaxMinData() {
		return maxMinData;
	}
	public void setMaxMinData(MinMaxData maxMinData) {
		this.maxMinData = maxMinData;
	}
	public DvHydrographReportMetadata getReportMetadata() {
		return reportMetadata;
	}
	public void setReportMetadata(DvHydrographReportMetadata reportMetadata) {
		this.reportMetadata = reportMetadata;
	}
	public TimeSeriesCorrectedData getComparisonSeries() {
		return comparisonSeries;
	}
	public void setComparisonSeries(TimeSeriesCorrectedData comparisonSeries) {
		this.comparisonSeries = comparisonSeries;
	}
	public List<FieldVisitMeasurement> getFieldVisitMeasurements() {
		return fieldVisitMeasurements;
	}
	public void setFieldVisitMeasurements(List<FieldVisitMeasurement> fieldVisitMeasurements) {
		this.fieldVisitMeasurements = fieldVisitMeasurements;
	}
	public TimeSeriesCorrectedData getFirstReferenceTimeSeries() {
		return firstReferenceTimeSeries;
	}
	public void setFirstReferenceTimeSeries(TimeSeriesCorrectedData firstReferenceTimeSeries) {
		this.firstReferenceTimeSeries = firstReferenceTimeSeries;
	}
	public TimeSeriesCorrectedData getFourthStatDerived() {
		return fourthStatDerived;
	}
	public void setFourthStatDerived(TimeSeriesCorrectedData fourthStatDerived) {
		this.fourthStatDerived = fourthStatDerived;
	}
	public List<WaterLevelRecord> getGwlevel() {
		return gwlevel;
	}
	public void setGwlevel(List<WaterLevelRecord> gwlevel) {
		this.gwlevel = gwlevel;
	}
	public List<Qualifier> getPrimarySeriesQualifiers() {
		return primarySeriesQualifiers;
	}
	public void setPrimarySeriesQualifiers(List<Qualifier> primarySeriesQualifiers) {
		this.primarySeriesQualifiers = primarySeriesQualifiers;
	}
	public List<Approval> getPrimarySeriesApprovals() {
		return primarySeriesApprovals;
	}
	public void setPrimarySeriesApprovals(List<Approval> primarySeriesApprovals) {
		this.primarySeriesApprovals = primarySeriesApprovals;
	}
	public TimeSeriesCorrectedData getSecondReferenceTimeSeries() {
		return secondReferenceTimeSeries;
	}
	public void setSecondReferenceTimeSeries(TimeSeriesCorrectedData secondReferenceTimeSeries) {
		this.secondReferenceTimeSeries = secondReferenceTimeSeries;
	}
	public TimeSeriesCorrectedData getSecondStatDerived() {
		return secondStatDerived;
	}
	public void setSecondStatDerived(TimeSeriesCorrectedData secondStatDerived) {
		this.secondStatDerived = secondStatDerived;
	}
	public String getSimsUrl() {
		return simsUrl;
	}
	public void setSimsUrl(String simsUrl) {
		this.simsUrl = simsUrl;
	}
	public TimeSeriesCorrectedData getThirdReferenceTimeSeries() {
		return thirdReferenceTimeSeries;
	}
	public void setThirdReferenceTimeSeries(TimeSeriesCorrectedData thirdReferenceTimeSeries) {
		this.thirdReferenceTimeSeries = thirdReferenceTimeSeries;
	}
	public TimeSeriesCorrectedData getThirdStatDerived() {
		return thirdStatDerived;
	}
	public void setThirdStatDerived(TimeSeriesCorrectedData thirdStatDerived) {
		this.thirdStatDerived = thirdStatDerived;
	}
	public String getWaterdataUrl() {
		return waterdataUrl;
	}
	public void setWaterdataUrl(String waterdataUrl) {
		this.waterdataUrl = waterdataUrl;
	}
	public List<WaterQualitySampleRecord> getWaterQuality() {
		return waterQuality;
	}
	public void setWaterQuality(List<WaterQualitySampleRecord> waterQuality) {
		this.waterQuality = waterQuality;
	}
}
