CREATE TABLE [dbo].[DimFX] (
    [date_key]           INT             NOT NULL,
    [rate_date]          DATE            NOT NULL,
    [usd_per_eur]        DECIMAL (18, 6) NOT NULL,
    [source_rate_date]   DATE            NOT NULL,
    [is_carried_forward] BIT             NOT NULL
);


GO

ALTER TABLE [dbo].[DimFX]
    ADD CONSTRAINT [FK_DimFX_DimDate] FOREIGN KEY ([date_key]) REFERENCES [dbo].[DimDate] ([date_key]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[DimFX]
    ADD CONSTRAINT [PK_DimFX] PRIMARY KEY NONCLUSTERED ([date_key] ASC) NOT ENFORCED;


GO