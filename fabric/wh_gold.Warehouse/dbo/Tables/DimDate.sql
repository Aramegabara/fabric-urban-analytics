CREATE TABLE [dbo].[DimDate] (
    [date_key]       INT          NULL,
    [full_date]      DATE         NULL,
    [year_number]    INT          NULL,
    [quarter_number] INT          NULL,
    [month_number]   INT          NULL,
    [month_name]     VARCHAR (10) NULL,
    [day_of_month]   INT          NULL,
    [day_name]       VARCHAR (10) NULL,
    [is_weekend]     INT          NOT NULL
);


GO