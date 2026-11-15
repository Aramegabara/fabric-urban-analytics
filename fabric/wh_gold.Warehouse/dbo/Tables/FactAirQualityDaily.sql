CREATE TABLE [dbo].[FactAirQualityDaily] (
    [date_key]     INT        NOT NULL,
    [sensor_id]    BIGINT     NOT NULL,
    [value_avg]    FLOAT (53) NOT NULL,
    [value_max]    FLOAT (53) NULL,
    [coverage_pct] FLOAT (53) NOT NULL
);


GO

ALTER TABLE [dbo].[FactAirQualityDaily]
    ADD CONSTRAINT [FK_FactAirQualityDaily_DimDate] FOREIGN KEY ([date_key]) REFERENCES [dbo].[DimDate] ([date_key]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[FactAirQualityDaily]
    ADD CONSTRAINT [FK_FactAirQualityDaily_DimSensor] FOREIGN KEY ([sensor_id]) REFERENCES [dbo].[DimSensor] ([sensor_id]) NOT ENFORCED;


GO