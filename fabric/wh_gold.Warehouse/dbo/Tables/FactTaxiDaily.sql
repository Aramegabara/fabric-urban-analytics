CREATE TABLE [dbo].[FactTaxiDaily] (
    [date_key]                   INT             NULL,
    [pickup_zone_id]             INT             NULL,
    [trip_count]                 INT             NULL,
    [total_distance_miles]       FLOAT (53)      NULL,
    [total_fare_usd]             DECIMAL (38, 2) NULL,
    [total_tip_usd]              DECIMAL (38, 2) NULL,
    [total_amount_usd]           DECIMAL (38, 2) NULL,
    [total_duration_seconds]     BIGINT          NULL,
    [total_passengers]           BIGINT          NULL,
    [trips_with_passenger_count] INT             NULL
);


GO