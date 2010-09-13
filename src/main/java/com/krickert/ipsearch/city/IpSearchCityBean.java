package com.krickert.ipsearch.city;

/**
 * Bean representation of a single row within the bean.
 * 
 * We've implemented a parser which takes in a single row. For each row it'll
 * process the fields we expect.
 * 
 * The goal here is to process these as fast as possible, so we will take in a
 * single row and go for each individual char of the string and parse the
 * columns to prevent a 2x in memory read. Probably overkill on the optimization
 * though.
 * 
 * Sample columns:
 * 
 * <pre>
 * "ip_start";"country_code";"country_name";"region_code";"region_name";"city";"zipcode";"latitude";"longitude";"metrocode"
 * </pre>
 * 
 * Sample data:
 * 
 * <pre>
 * "3523140760";"US";"United States";"17";"Illinois";"Chicago";"60611";"41.9288";"-87.6315";"602"
 * "3523140848";"US";"United States";"17";"Illinois";"Chicago";"60657";"41.9373";"-87.6551";"602"
 * </pre>
 * 
 * @author krickert
 * 
 */
public class IpSearchCityBean {

  private Long ipStart;// minlen 1 max len 10 max value 4278190080
  private String countryCode;// max length 2 required
  private String countryName;// min length 4 max length 32 required
  private String regionCode;// max length 2 optional
  private String regionName;// min length 2 max length 41 optional
  private String city; // min length 1 max length 34 optional
  private String zipCode;// min length 2 max length 6 optional
  private Double lat;// min max required
  private Double lon;// min max required
  private String metroCode;// a length 3 optional

  public Long getIpStart() {
    return ipStart;
  }

  public void setIpStart(Long ipStart) {
    this.ipStart = ipStart;
  }

  public String getCountryCode() {
    return countryCode;
  }

  public void setCountryCode(String countryCode) {
    this.countryCode = countryCode;
  }

  public String getCountryName() {
    return countryName;
  }

  public void setCountryName(String countryName) {
    this.countryName = countryName;
  }

  public String getRegionCode() {
    return regionCode;
  }

  public void setRegionCode(String regionCode) {
    this.regionCode = regionCode;
  }

  public String getRegionName() {
    return regionName;
  }

  public void setRegionName(String regionName) {
    this.regionName = regionName;
  }

  public String getCity() {
    return city;
  }

  public void setCity(String city) {
    this.city = city;
  }

  public String getZipCode() {
    return zipCode;
  }

  public void setZipCode(String zipCode) {
    this.zipCode = zipCode;
  }

  public Double getLat() {
    return lat;
  }

  public void setLat(Double lat) {
    this.lat = lat;
  }

  public Double getLon() {
    return lon;
  }

  public void setLon(Double lon) {
    this.lon = lon;
  }

  public String getMetroCode() {
    return metroCode;
  }

  public void setMetroCode(String metroCode) {
    this.metroCode = metroCode;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("IpSearchCityBean [ipStart=");
    builder.append(ipStart);
    builder.append(", countryCode=");
    builder.append(countryCode);
    builder.append(", countryName=");
    builder.append(countryName);
    builder.append(", regionCode=");
    builder.append(regionCode);
    builder.append(", regionName=");
    builder.append(regionName);
    builder.append(", city=");
    builder.append(city);
    builder.append(", zipCode=");
    builder.append(zipCode);
    builder.append(", lat=");
    builder.append(lat);
    builder.append(", lon=");
    builder.append(lon);
    builder.append(", metroCode=");
    builder.append(metroCode);
    builder.append("]");
    return builder.toString();
  }

}
