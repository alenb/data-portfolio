# Power BI DAX Measures Script
# Melbourne Transport Resilience Dashboard
# Copy and paste these measures into Power BI Desktop

## Core Performance KPIs

### 1. On-Time Performance Rate
```dax
On_Time_Rate = 
DIVIDE(
    COUNTROWS(FILTER(departures_curated_20250706_001548, departures_curated_20250706_001548[delay_minutes] <= 2)),
    COUNTROWS(departures_curated_20250706_001548)
) * 100
```

### 2. Average Delay in Minutes  
```dax
Avg_Delay_Minutes = AVERAGE(departures_curated_20250706_001548[delay_minutes])
```

### 3. Total Departures
```dax
Total_Departures = COUNTROWS(departures_curated_20250706_001548)
```

### 4. High Risk Routes Count
```dax
High_Risk_Routes = 
COUNTROWS(
    FILTER(disruption_risk_index_20250706_001548, 
           disruption_risk_index_20250706_001548[disruption_risk_index] > 50)
)
```

### 5. Average DRI Score
```dax
Avg_DRI_Score = AVERAGE(disruption_risk_index_20250706_001548[disruption_risk_index])
```

## Advanced Measures

### 6. Service Reliability Rating
```dax
Service_Reliability = 
SWITCH(
    TRUE(),
    [On_Time_Rate] >= 95, "Excellent",
    [On_Time_Rate] >= 85, "Good", 
    [On_Time_Rate] >= 70, "Fair",
    "Poor"
)
```

### 7. Peak Hour Performance
```dax
Peak_Hour_On_Time = 
CALCULATE(
    [On_Time_Rate],
    OR(
        departures_curated_20250706_001548[scheduled_hour] >= 7 && departures_curated_20250706_001548[scheduled_hour] <= 9,
        departures_curated_20250706_001548[scheduled_hour] >= 17 && departures_curated_20250706_001548[scheduled_hour] <= 19
    )
)
```

### 8. Major Delays Count
```dax
Major_Delays = 
COUNTROWS(
    FILTER(departures_curated_20250706_001548, 
           departures_curated_20250706_001548[delay_minutes] > 15)
)
```

### 9. Station Performance Rank
```dax
Station_Performance = 
CALCULATE(
    [On_Time_Rate],
    ALLEXCEPT(departures_curated_20250706_001548, departures_curated_20250706_001548[station_name])
)
```

### 10. Route Type Performance
```dax
Route_Type_Avg_Delay = 
CALCULATE(
    [Avg_Delay_Minutes],
    ALLEXCEPT(routes_curated_20250706_001548, routes_curated_20250706_001548[route_type_name])
)
```

## Creation Order (Recommended):
1. Start with measures #1-3 (basic KPIs)
2. Test them in a simple table visual
3. Add measures #4-5 (risk analysis)  
4. Create advanced measures #6-10 once basics work

## Troubleshooting Tips:
- If you get table name errors, check the exact table names in your model
- Make sure column names match exactly (case sensitive)
- Test each measure individually before moving to the next
- Use a simple table visual to validate measure calculations

## Visual Testing:
Create a simple table with:
- Routes[route_name] 
- [On_Time_Rate]
- [Avg_Delay_Minutes]
- [Avg_DRI_Score]

This will validate your measures are working correctly before building complex visuals.
