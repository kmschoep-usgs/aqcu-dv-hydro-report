package gov.usgs.aqcu.model;

import java.math.BigDecimal;
import java.time.Instant;

/** 
 * This is an abbreviated version of com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.DischargeSummary
 * It has:
 * a) Several fields renamed;
 *      1) MeasurementId -> measurementNumber;
 *      2) MeasurementStartTime -> measurementStrtDate;
 * b) Discharge from the rounded value;
 * c) Error range calculated from the rounded value:
 *      1) errorMinDischarge;
 *      2) errorMaxDischarge
 */
public class FieldVisitMeasurement {

	//Required Properties
	private BigDecimal discharge;
	private Instant measurementStartDate;
	private BigDecimal errorMinDischarge;
	private BigDecimal errorMaxDischarge;
	private String measurementNumber;
	private Boolean publish;

	public FieldVisitMeasurement(){
	}

	public FieldVisitMeasurement(String measurementNumber, BigDecimal dischargeValue, BigDecimal errorMaxDischarge,
			BigDecimal errorMinDischarge, Instant measurementStartDate, Boolean publish) {
		this.measurementNumber = measurementNumber;
		this.discharge = dischargeValue;
		this.errorMinDischarge = errorMinDischarge;
		this.errorMaxDischarge = errorMaxDischarge;
		this.measurementStartDate = measurementStartDate;
		this.publish = publish;
	}

	public BigDecimal getDischarge() {
		return discharge;
	}
	public void setDischarge(BigDecimal discharge) {
		this.discharge = discharge;
	}
	public Instant getMeasurementStartDate() {
		return measurementStartDate;
	}
	public void setMeasurementStartDate(Instant measurementStartDate) {
		this.measurementStartDate = measurementStartDate;
	}
	public BigDecimal getErrorMinDischarge() {
		return errorMinDischarge;
	}
	public void setErrorMinDischarge(BigDecimal errorMinDischarge) {
		this.errorMinDischarge = errorMinDischarge;
	}
	public BigDecimal getErrorMaxDischarge() {
		return errorMaxDischarge;
	}
	public void setErrorMaxDischarge(BigDecimal errorMaxDischarge) {
		this.errorMaxDischarge = errorMaxDischarge;
	}
	public String getMeasurementNumber() {
		return measurementNumber;
	}
	public void setMeasurementNumber(String measurementNumber) {
		this.measurementNumber = measurementNumber;
	}
	public Boolean isPublish() {
		return publish;
	}
	public void setPublish(Boolean publish) {
		this.publish = publish;
	}
}
