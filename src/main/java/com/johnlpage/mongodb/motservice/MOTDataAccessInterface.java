package com.johnlpage.mongodb.motservice;



public interface MOTDataAccessInterface {
    public String getMOTResultInJSON(String identifier);
    public long[] getVehicleIdentifiers();
    public boolean initialised();
    public boolean createNewMOTResult(Long testId,Long vehicleid);
    public void resetTestDatabase() ;
    public boolean updateMOTResult(Long vehicleid);

}