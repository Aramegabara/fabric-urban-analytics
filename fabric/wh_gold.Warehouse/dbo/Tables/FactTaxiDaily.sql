CREATE TABLE [dbo].[FactTaxiDaily] (
    [date_key]                   INT             NOT NULL,
    [pickup_zone_id]             INT             NOT NULL,
    [trip_count]                 BIGINT          NOT NULL,
    [total_distance_miles]       DECIMAL (18, 2) NOT NULL,
    [total_fare_usd]             DECIMAL (18, 2) NOT NULL,
    [total_tip_usd]              DECIMAL (18, 2) NOT NULL,
    [total_amount_usd]           DECIMAL (18, 2) NOT NULL,
    [total_duration_seconds]     BIGINT          NOT NULL,
    [total_passengers]           BIGINT          NULL,
    [trips_with_passenger_count] BIGINT          NOT NULL
);


GO

ALTER TABLE [dbo].[FactTaxiDaily]
    ADD CONSTRAINT [FK_FactTaxiDaily_DimDate] FOREIGN KEY ([date_key]) REFERENCES [dbo].[DimDate] ([date_key]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[FactTaxiDaily]
    ADD CONSTRAINT [FK_FactTaxiDaily_DimZone] FOREIGN KEY ([pickup_zone_id]) REFERENCES [dbo].[DimZone] ([zone_id]) NOT ENFORCED;


GO