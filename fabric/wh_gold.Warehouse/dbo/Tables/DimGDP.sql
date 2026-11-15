CREATE TABLE [dbo].[DimGDP] (
    [country_code] VARCHAR (3)     NOT NULL,
    [year_number]  SMALLINT        NOT NULL,
    [gdp_usd]      DECIMAL (20, 2) NOT NULL
);


GO

ALTER TABLE [dbo].[DimGDP]
    ADD CONSTRAINT [PK_DimGDP] PRIMARY KEY NONCLUSTERED ([country_code] ASC, [year_number] ASC) NOT ENFORCED;


GO