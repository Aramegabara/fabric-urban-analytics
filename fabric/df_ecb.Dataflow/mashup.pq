[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "ecb_fx_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared ecb_fx = let
    Source = Csv.Document(
        Web.Contents(
            "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A",
            [Query = [format = "csvdata", startPeriod = "2024-01-01", endPeriod = "2026-11-01"]]
        ),
        [Delimiter = ",", QuoteStyle = QuoteStyle.Csv]
    ),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    Selected = Table.SelectColumns(Promoted, {"TIME_PERIOD", "OBS_VALUE"})
in
    Selected;
shared ecb_fx_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = false, EnableVorder = true, OutputMetadataRefresh = true, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "c70900d4-8446-43f4-8a7d-4bca5f785f14"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "ba9cfdb7-cf33-423d-8a4c-21d224e17ea8"]}[Data],
  TableNavigation = Navigation_2{[Id = "ecb_fx", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
