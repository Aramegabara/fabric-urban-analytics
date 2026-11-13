CREATE TABLE [dbo].[DimSensor] (
    [sensor_id]    BIGINT        NULL,
    [station_name] VARCHAR (100) NULL,
    [borough]      VARCHAR (50)  NULL,
    [parameter]    VARCHAR (20)  NULL,
    [latitude]     FLOAT (53)    NULL,
    [longitude]    FLOAT (53)    NULL
);


GO