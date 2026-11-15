CREATE TABLE [dbo].[DimSensor] (
    [sensor_id]    BIGINT        NOT NULL,
    [station_name] VARCHAR (100) NOT NULL,
    [borough]      VARCHAR (50)  NOT NULL,
    [parameter]    VARCHAR (20)  NOT NULL,
    [latitude]     FLOAT (53)    NOT NULL,
    [longitude]    FLOAT (53)    NOT NULL
);


GO

ALTER TABLE [dbo].[DimSensor]
    ADD CONSTRAINT [PK_DimSensor] PRIMARY KEY NONCLUSTERED ([sensor_id] ASC) NOT ENFORCED;


GO