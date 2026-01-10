package com.manish.buildbasket.api.dto;

public class PlayerAnalyticsDTO {

    private int playerId;
    private String playerName;
    private int age;
    private int pos;
    private String team;
    private int archetype;
    private int state;
    private double shooting;
    private double playmaking;
    private double rebounding;
    private double interiorDefense;
    private double perimeterDefense;
    private double scoring;
    private double efficiency;
    private double impact;

    // Constructor
    public PlayerAnalyticsDTO(int playerId, String playerName, int age, int pos, String team,
                              int archetype, int state, double shooting, double playmaking, double rebounding, double interiorDefense, 
                              double perimeterDefense, double scoring, double efficiency, double impact) {
        this.playerId = playerId;
        this.playerName = playerName;
        this.age = age;
        this.pos = pos;
        this.team = team;
        this.archetype = archetype;
        this.state = state;
        this.shooting = shooting;
        this.playmaking = playmaking;
        this.rebounding = rebounding;
        this.interiorDefense = interiorDefense;
        this.perimeterDefense = perimeterDefense;
        this.scoring = scoring;
        this.efficiency = efficiency;
        this.impact = impact;
    }

    // Getters and Setters

    public int getPlayerId() {
        return playerId;
    }

    public void setPlayerId(int playerId) {
        this.playerId = playerId;
    }

    public String getPlayerName() {
        return playerName;
    }

    public void setPlayerName(String playerName) {
        this.playerName = playerName;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getPos() {
        return pos;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }

    public String getTeam() {
        return team;
    }

    public void setTeam(String team) {
        this.team = team;
    }

    public int getArchetype() {
        return archetype;
    }

    public void setArchetype(int archetype) {
        this.archetype = archetype;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public double getShooting() {
        return shooting;
    }

    public void setShooting(double shooting) {
        this.shooting = shooting;
    }

    public double getPlaymaking() {
        return playmaking;
    }

    public void setPlaymaking(double playmaking) {
        this.playmaking = playmaking;
    }

    public double getRebounding() {
        return rebounding;
    }

    public void setRebounding(double rebounding) {
        this.rebounding = rebounding;
    }

    public double getInteriorDefense() {
        return interiorDefense;
    }

    public void setInteriorDefense(double interiorDefense) {
        this.interiorDefense = interiorDefense;
    }

    public double getPerimeterDefense() {
        return perimeterDefense;
    }

    public void setPerimeterDefense(double perimeterDefense) {
        this.perimeterDefense = perimeterDefense;
    }

    public double getScoring() {
        return scoring;
    }

    public void setScoring(double scoring) {
        this.scoring = scoring;
    }

    public double getEfficiency() {
        return efficiency;
    }

    public void setEfficiency(double efficiency) {
        this.efficiency = efficiency;
    }

    public double getImpact() {
        return impact;
    }

    public void setImpact(double impact) {
        this.impact = impact;
    }
}
