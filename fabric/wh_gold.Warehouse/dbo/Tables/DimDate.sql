CREATE TABLE [dbo].[DimDate] (
    [date_key]       INT          NOT NULL,
    [full_date]      DATE         NOT NULL,
    [year_number]    SMALLINT     NOT NULL,
    [quarter_number] SMALLINT     NOT NULL,
    [month_number]   SMALLINT     NOT NULL,
    [month_name]     VARCHAR (10) NOT NULL,
    [day_of_month]   SMALLINT     NOT NULL,
    [day_name]       VARCHAR (10) NOT NULL,
    [is_weekend]     BIT          NOT NULL
);


GO

ALTER TABLE [dbo].[DimDate]
    ADD CONSTRAINT [PK_DimDate] PRIMARY KEY NONCLUSTERED ([date_key] ASC) NOT ENFORCED;


GO