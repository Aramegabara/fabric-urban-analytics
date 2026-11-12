[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "worldbank_gdp_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared worldbank_gdp = let
    Source = Json.Document(
        Web.Contents(
            "https://api.worldbank.org/v2",
            [
                RelativePath = "country/USA/indicator/NY.GDP.MKTP.CD",
                Query = [format = "json", per_page = "100"]
            ]
        )
    ),
    Records = Source{1},
    Converted = Table.FromList(Records, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    Expanded = Table.ExpandRecordColumn(
        Converted,
        "Column1",
        {"countryiso3code", "date", "value"}
    ),
    Typed = Table.TransformColumnTypes(
        Expanded,
        {
            {"countryiso3code", type text},
            {"date", type text},
            {"value", type nullable number}
        }
    )
in
    Typed;
shared worldbank_gdp_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = false, EnableVorder = true, OutputMetadataRefresh = true, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "c70900d4-8446-43f4-8a7d-4bca5f785f14"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "ba9cfdb7-cf33-423d-8a4c-21d224e17ea8"]}[Data],
  TableNavigation = Navigation_2{[Id = "worldbank_gdp", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
