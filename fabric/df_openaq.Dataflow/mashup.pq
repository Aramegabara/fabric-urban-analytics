[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared ApiKey = let
    Source = Lakehouse.Contents([]),
    Workspace = Source{[workspaceId = "c70900d4-8446-43f4-8a7d-4bca5f785f14"]}[Data],
    Bronze = Workspace{[lakehouseId = "ba9cfdb7-cf33-423d-8a4c-21d224e17ea8"]}[Data],
    Files = Bronze{[Id = "Files", ItemKind = "Folder"]}[Data],
    Config = Files{[Name = "config"]}[Content],
    KeyFile = Config{[Name = "openaq.key"]}[Content],
    ApiKey = Text.Trim(Text.FromBinary(KeyFile))
in
    ApiKey;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "openaq_days_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared openaq_days = let
    Sensors = {673, 1097, 1102, 1103, 1128, 1145, 1146, 1152, 25520, 2780575, 1535, 23341},
    DateFrom = "2024-01-01",
    DateTo = "2026-11-01",
    PageSize = 1000,

    GetPage = (sensorId as number, page as number) as list =>
        let
            Fetch = () =>
                let
                    Response = Web.Contents(
                        "https://api.openaq.org/v3",
                        [
                            RelativePath = "sensors/" & Number.ToText(sensorId) & "/days",
                            Query = [
                                date_from = DateFrom,
                                date_to = DateTo,
                                limit = Number.ToText(PageSize),
                                page = Number.ToText(page)
                            ],
                            Headers = [#"X-API-Key" = ApiKey],
                            ManualStatusHandling = {429, 500, 502, 503, 504}
                        ]
                    ),
                    Status = Value.Metadata(Response)[Response.Status]
                in
                    if Status = 200 then Json.Document(Response)[results] else null,

            Attempts = List.Generate(
                () => [try_number = 1, result = Fetch()],
                each [try_number] <= 5,
                each [
                    try_number = [try_number] + 1,
                    result =
                        if [result] <> null then
                            [result]
                        else
                            Function.InvokeAfter(
                                Fetch,
                                #duration(0, 0, 0, Number.Power(2, [try_number]))
                            )
                ],
                each [result]
            ),

            Final = List.Last(Attempts)
        in
            if Final = null then
                error "OpenAQ failed for sensor " & Number.ToText(sensorId)
            else
                Final,

    GetSensor = (sensorId as number) as list =>
        let
            Pages = List.Generate(
                () => [page = 1, rows = GetPage(sensorId, 1)],
                each List.Count([rows]) > 0,
                each [
                    page = [page] + 1,
                    rows = if List.Count([rows]) < PageSize
                           then {}
                           else GetPage(sensorId, [page] + 1)
                ],
                each [rows]
            ),
            Flat = List.Combine(Pages)
        in
            List.Transform(Flat, each Record.AddField(_, "sensor_id", sensorId)),

    AllRows = List.Combine(List.Transform(Sensors, GetSensor)),

    Shaped = Table.FromRecords(
        List.Transform(AllRows, each [
            sensor_id = [sensor_id],
            value = [value],
            parameter_id = [parameter][id],
            parameter_name = [parameter][name],
            parameter_units = [parameter][units],
            period_label = [period][label],
            period_interval = [period][interval],
            period_datetimeFrom_utc = [period][datetimeFrom][utc],
            period_datetimeFrom_local = [period][datetimeFrom][local],
            period_datetimeTo_utc = [period][datetimeTo][utc],
            period_datetimeTo_local = [period][datetimeTo][local],
            summary_min = [summary][min],
            summary_q25 = [summary][q25],
            summary_median = [summary][median],
            summary_q75 = [summary][q75],
            summary_max = [summary][max],
            summary_avg = [summary][avg],
            summary_sd = [summary][sd],
            coverage_expectedCount = [coverage][expectedCount],
            coverage_observedCount = [coverage][observedCount],
            coverage_percentComplete = [coverage][percentComplete],
            coverage_percentCoverage = [coverage][percentCoverage]
        ])
    ),

    Typed = Table.TransformColumnTypes(
        Shaped,
        {
            {"sensor_id", Int64.Type},
            {"value", type nullable number},
            {"parameter_id", Int64.Type},
            {"parameter_name", type text},
            {"parameter_units", type text},
            {"period_label", type text},
            {"period_interval", type text},
            {"period_datetimeFrom_utc", type text},
            {"period_datetimeFrom_local", type text},
            {"period_datetimeTo_utc", type text},
            {"period_datetimeTo_local", type text},
            {"summary_min", type nullable number},
            {"summary_q25", type nullable number},
            {"summary_median", type nullable number},
            {"summary_q75", type nullable number},
            {"summary_max", type nullable number},
            {"summary_avg", type nullable number},
            {"summary_sd", type nullable number},
            {"coverage_expectedCount", Int64.Type},
            {"coverage_observedCount", Int64.Type},
            {"coverage_percentComplete", type nullable number},
            {"coverage_percentCoverage", type nullable number}
        }
    )
in
    Typed;
shared openaq_days_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = false, EnableVorder = true, OutputMetadataRefresh = true, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "c70900d4-8446-43f4-8a7d-4bca5f785f14"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "ba9cfdb7-cf33-423d-8a4c-21d224e17ea8"]}[Data],
  TableNavigation = Navigation_2{[Id = "openaq_days", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "openaq_sensors_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared openaq_sensors = let
    Bbox = "-74.5,40.4,-73.6,41.0",
    Sensors = {673, 1097, 1102, 1103, 1128, 1145, 1146, 1152, 25520, 2780575, 1535, 23341},

    Boroughs = #table(
        {"sensor_id", "borough"},
        {
            {673, "Manhattan"},
            {1097, "Manhattan"},
            {1102, "Bronx"},
            {1103, "Queens"},
            {1128, "Brooklyn"},
            {1145, "Brooklyn"},
            {1146, "Bronx"},
            {1152, "Queens"},
            {1535, "New Jersey"},
            {23341, "New Jersey"},
            {25520, "Queens"},
            {2780575, "Staten Island"}
        }
    ),

    Locations = Json.Document(
        Web.Contents(
            "https://api.openaq.org/v3",
            [
                RelativePath = "locations",
                Query = [bbox = Bbox, limit = "1000"],
                Headers = [#"X-API-Key" = ApiKey]
            ]
        )
    )[results],

    Rows = List.Combine(
        List.Transform(
            Locations,
            (location) =>
                List.Transform(
                    location[sensors],
                    (sensor) => [
                        sensor_id = sensor[id],
                        parameter = sensor[parameter][name],
                        station_name = location[name],
                        provider = location[provider][name],
                        latitude = location[coordinates][latitude],
                        longitude = location[coordinates][longitude]
                    ]
                )
        )
    ),

    Shaped = Table.FromRecords(Rows),
    Filtered = Table.SelectRows(Shaped, each List.Contains(Sensors, [sensor_id])),
    WithBorough = Table.Join(Filtered, "sensor_id", Boroughs, "sensor_id", JoinKind.Inner),

    Typed = Table.TransformColumnTypes(
        WithBorough,
        {
            {"sensor_id", Int64.Type},
            {"parameter", type text},
            {"station_name", type text},
            {"provider", type text},
            {"borough", type text},
            {"latitude", type number},
            {"longitude", type number}
        }
    )
in
    Typed;
shared openaq_sensors_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = false, EnableVorder = true, OutputMetadataRefresh = true, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "c70900d4-8446-43f4-8a7d-4bca5f785f14"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "ba9cfdb7-cf33-423d-8a4c-21d224e17ea8"]}[Data],
  TableNavigation = Navigation_2{[Id = "openaq_sensors", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
