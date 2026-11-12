[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "taxi_zones_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared taxi_zones = let
    Source = Csv.Document(
        Web.Contents(
            "https://d37ci6vzurychx.cloudfront.net",
            [RelativePath = "misc/taxi_zone_lookup.csv"]
        ),
        [Delimiter = ",", QuoteStyle = QuoteStyle.Csv]
    ),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    Typed = Table.TransformColumnTypes(
        Promoted,
        {
            {"LocationID", Int64.Type},
            {"Borough", type text},
            {"Zone", type text},
            {"service_zone", type text}
        }
    )
in
    Typed;
shared taxi_zones_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = false, EnableVorder = true, OutputMetadataRefresh = true, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "c70900d4-8446-43f4-8a7d-4bca5f785f14"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "ba9cfdb7-cf33-423d-8a4c-21d224e17ea8"]}[Data],
  TableNavigation = Navigation_2{[Id = "taxi_zones", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
