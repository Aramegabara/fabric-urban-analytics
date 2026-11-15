CREATE TABLE [dbo].[DimZone] (
    [zone_id]      INT           NOT NULL,
    [borough]      VARCHAR (50)  NOT NULL,
    [zone_name]    VARCHAR (100) NOT NULL,
    [service_zone] VARCHAR (50)  NULL
);


GO

ALTER TABLE [dbo].[DimZone]
    ADD CONSTRAINT [PK_DimZone] PRIMARY KEY NONCLUSTERED ([zone_id] ASC) NOT ENFORCED;


GO