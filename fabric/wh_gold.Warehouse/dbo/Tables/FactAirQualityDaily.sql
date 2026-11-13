CREATE TABLE [dbo].[FactAirQualityDaily] (
    [date_key]     INT        NULL,
    [sensor_id]    BIGINT     NULL,
    [value_avg]    FLOAT (53) NULL,
    [value_max]    FLOAT (53) NULL,
    [coverage_pct] FLOAT (53) NULL
);


GO